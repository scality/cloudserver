const url = require('url');
const async = require('async');

const { auth, errors } = require('arsenal');
const { responseJSONBody } = require('arsenal').s3routes.routesUtils;
const vault = require('../auth/vault');
const metadata = require('../metadata/wrapper');
const locationConstraintCheck = require(
    '../api/apiUtils/object/locationConstraintCheck');
const { dataStore } = require('../api/apiUtils/object/storeObject');
const prepareRequestContexts = require(
'../api/apiUtils/authorization/prepareRequestContexts');
const { metadataValidateBucketAndObj } = require('../metadata/metadataUtils');

auth.setHandler(vault);

const NAMESPACE = 'default';
const CIPHER = null; // replication/lifecycle does not work on encrypted objects

function _decodeURI(uri) {
    // do the same decoding than in S3 server
    return decodeURIComponent(uri.replace(/\+/g, ' '));
}

function normalizeBackbeatRequest(req) {
    /* eslint-disable no-param-reassign */
    const parsedUrl = url.parse(req.url, true);
    req.path = _decodeURI(parsedUrl.pathname);
    const pathArr = req.path.split('/');
    req.query = parsedUrl.query;
    req.resourceType = pathArr[3];
    req.bucketName = pathArr[4];
    req.objectKey = pathArr.slice(5).join('/');
    /* eslint-enable no-param-reassign */
}

function _respond(response, payload, log, callback) {
    const body = typeof payload === 'object' ?
        JSON.stringify(payload) : payload;
    const httpHeaders = {
        'x-amz-id-2': log.getSerializedUids(),
        'x-amz-request-id': log.getSerializedUids(),
        'content-type': 'application/json',
        'content-length': Buffer.byteLength(body),
    };
    response.writeHead(200, httpHeaders);
    response.end(body, 'utf8', callback);
}

function _getRequestPayload(req, cb) {
    const payload = [];
    let payloadLen = 0;
    req.on('data', chunk => {
        payload.push(chunk);
        payloadLen += chunk.length;
    }).on('error', cb)
    .on('end', () => cb(null, Buffer.concat(payload, payloadLen).toString()));
}

/*
PUT /_/backbeat/metadata/<bucket name>/<object key>
PUT /_/backbeat/data/<bucket name>/<object key>
*/

function putData(request, response, bucketInfo, objMd, log, callback) {
    const canonicalID = request.headers['x-scal-canonical-id'];
    if (canonicalID === undefined) {
        log.error('bad request: missing x-scal-canonical-id header');
        return callback(errors.BadRequest);
    }
    const contentMD5 = request.headers['content-md5'];
    if (contentMD5 === undefined) {
        log.error('bad request: missing content-md5 header');
        return callback(errors.BadRequest);
    }
    const context = {
        bucketName: request.bucketName,
        owner: canonicalID,
        namespace: NAMESPACE,
        objectKey: request.objectKey,
    };
    const payloadLen = parseInt(request.headers['content-length'], 10);
    const backendInfoObj = locationConstraintCheck(
        request, null, bucketInfo, log);
    if (backendInfoObj.err) {
        log.error('error getting backendInfo', {
            error: backendInfoObj.err,
            method: 'routeBackbeat',
        });
        return callback(errors.InternalError);
    }
    const backendInfo = backendInfoObj.backendInfo;
    return dataStore(
        context, CIPHER, request, payloadLen, {},
        backendInfo, log, (err, retrievalInfo, md5) => {
            if (err) {
                return callback(err);
            }
            if (contentMD5 !== md5) {
                return callback(errors.BadDigest);
            }
            const { key, dataStoreName } = retrievalInfo;
            const dataRetrievalInfo = [{
                key,
                dataStoreName,
            }];
            return _respond(response, dataRetrievalInfo, log, callback);
        });
}

function putMetadata(request, response, bucketInfo, objMd, log, callback) {
    return _getRequestPayload(request, (err, payload) => {
        if (err) {
            return callback(err);
        }
        let omVal;
        try {
            omVal = JSON.parse(payload);
        } catch (err) {
            // FIXME: add error type MalformedJSON
            return callback(errors.MalformedPOSTRequest);
        }
        const { headers, bucketName, objectKey } = request;
        // check if it's metadata only operation
        if (headers['x-scal-replication-content'] === 'METADATA') {
            if (!objMd) {
                // if the target does not exist, return an error to
                // backbeat, who will have to retry the operation as a
                // complete replication
                return callback(errors.ObjNotFound);
            }
            // use original data locations
            omVal.location = objMd.location;
        }
        // specify both 'versioning' and 'versionId' to create a "new"
        // version (updating master as well) but with specified
        // versionId
        const options = {
            versioning: true,
            versionId: omVal.versionId,
        };
        log.trace('putting object version', {
            objectKey: request.objectKey, omVal, options });
        return metadata.putObjectMD(bucketName, objectKey, omVal, options, log,
            (err, md) => {
                if (err) {
                    return callback(err);
                }
                return _respond(response, md, log, callback);
            });
    });
}


const backbeatRoutes = {
    PUT: { data: putData,
        metadata: putMetadata },
};

function routeBackbeat(clientIP, request, response, log) {
    log.debug('routing request', { method: 'routeBackbeat' });
    normalizeBackbeatRequest(request);
    const invalidRequest = (!request.bucketName ||
                            !request.objectKey ||
                            !request.resourceType);
    if (invalidRequest) {
        log.debug('invalid request', {
            method: request.method, bucketName: request.bucketName,
            objectKey: request.objectKey, resourceType: request.resourceType,
        });
        return responseJSONBody(errors.MethodNotAllowed, null, response, log);
    }
    const requestContexts = prepareRequestContexts('objectReplicate',
                                                   request);
    return async.waterfall([next => auth.server.doAuth(
        request, log, (err, userInfo) => {
            if (err) {
                log.debug('authentication error', {
                    error: err,
                    method: request.method,
                    bucketName: request.bucketName,
                    objectKey: request.objectKey,
                });
            }
            return next(err, userInfo);
        }, 's3', requestContexts),
        (userInfo, next) => {
            const mdValParams = { bucketName: request.bucketName,
                objectKey: request.objectKey,
                authInfo: userInfo,
                requestType: 'ReplicateObject' };
            return metadataValidateBucketAndObj(mdValParams, log, next);
        },
        (bucketInfo, objMd, next) => {
            if (backbeatRoutes[request.method] === undefined ||
                backbeatRoutes[request.method][request.resourceType]
                === undefined) {
                log.debug('no such route', { method: request.method,
                    bucketName: request.bucketName,
                    objectKey: request.objectKey,
                    resourceType:
                                             request.resourceType });
                return next(errors.MethodNotAllowed);
            }
            const versioningConfig = bucketInfo.getVersioningConfiguration();
            if (!versioningConfig || versioningConfig.Status !== 'Enabled') {
                log.debug('bucket versioning is not enabled',
                    { method: request.method,
                        bucketName: request.bucketName,
                        objectKey: request.objectKey,
                        resourceType: request.resourceType });
                return next(errors.InvalidBucketState);
            }
            return backbeatRoutes[request.method][request.resourceType](
                request, response, bucketInfo, objMd, log, next);
        }],
        err => {
            if (err) {
                return responseJSONBody(err, null, response, log);
            }
            log.debug('backbeat route response sent successfully',
                { method: request.method,
                    bucketName: request.bucketName,
                    objectKey: request.objectKey });
            return undefined;
        });
}


module.exports = routeBackbeat;
