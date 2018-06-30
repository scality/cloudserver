const url = require('url');
const async = require('async');

const { auth, errors, s3routes } = require('arsenal');
const { responseJSONBody } = s3routes.routesUtils;
const vault = require('../auth/vault');

const prepareRequestContexts = require(
'../api/apiUtils/authorization/prepareRequestContexts');
const { decodeVersionId } = require('../api/apiUtils/object/versioning');

const metadata = require('../metadata/wrapper');
auth.setHandler(vault);

function normalizeMetadataRequest(req) {
    /* eslint-disable no-param-reassign */
    const parsedUrl = url.parse(req.url, true);
    req.path = parsedUrl.pathname;
    req.query = parsedUrl.query;
    const pathArr = req.path.split('/');
    req.resourceType = pathArr[3];
    req.bucketName = pathArr[4];
    if (pathArr[5]) {
        req.objectKey = pathArr.slice(5).join('/');
    }
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

/*
- getting list of buckets for each raft session -
GET /_/metadata/listbuckets/<raft session id>
- getting list of objects for each bucket -
GET /_/metadata/listobjects/<bucket name>
- getting metadata for bucket -
GET /_/metadata/getbucket/<bucket name>
- getting metadata for object -
GET /_/metadata/getobject/<bucket name>/<object key>
*/

function getRaftBuckets(request, response, raftId, objectKey, log, callback) {
    return metadata.getRaftBuckets(raftId, log, (err, res) => {
        if (err) {
            // TODO: figure out correct error to respond with
            return callback(err);
        }
        return _respond(response, res, log, callback);
    });
}

function getBucketMetadata(request, response, bucketName, objectKey, log,
    callback) {
    return metadata.getBucketAttributes(bucketName, log, (err, res) => {
        if (err) {
            // TODO: figure out correct error to respond with
            return callback(err);
        }
        return _respond(response, res, log, callback);
    });
}

function getObjectList(request, response, bucketName, objectKey, log,
    callback) {
    return metadata.listObject(bucketName, { listingType: 'Delimiter' }, log,
    (err, res) => {
        if (err) {
            return callback(err);
        }
        return _respond(response, res, log, callback);
    });
}

function getObjectMetadata(request, response, bucketName, objectKey,
log, callback) {
    return metadata.getObject(bucketName, objectKey, { }, log, (err, res) => {
        if (err) {
            return callback(err);
        }
        return _respond(response, res, log, callback);
    });
}

const metadataRoutes = {
    GET: {
        listbuckets: getRaftBuckets,
        listobjects: getObjectList,
        getbucket: getBucketMetadata,
        getobject: getObjectMetadata,
    },
};

function routeMetadata(clientIP, request, response, log) {
    log.debug('routing request', { method: 'routeBackbeat' });
    normalizeMetadataRequest(request);
    const invalidRequest = ((!request.resourceType || !request.bucketName) ||
        (request.resourceType === 'getobject' && !request.objectKey));
    if (invalidRequest) {
        log.debug('invalid request', {
            method: request.method, bucketName: request.bucketName,
            objectKey: request.objectKey, resourceType: request.resourceType,
            query: request.query,
        });
        return responseJSONBody(errors.MethodNotAllowed, null, response, log);
    }
    const requestContexts = prepareRequestContexts('objectReplicate', request);
    const decodedVidResult = decodeVersionId(request.query);
    if (decodedVidResult instanceof Error) {
        log.trace('invalid versionId query', {
            versionId: request.query.versionId,
            error: decodedVidResult,
        });
        return responseJSONBody(errors.InvalidArgument, null, response, log);
    }
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
            const invalidRoute = metadataRoutes[request.method] === undefined ||
                metadataRoutes[request.method][request.resourceType] ===
                    undefined;
            if (invalidRoute) {
                log.debug('no such route', { method: request.method,
                    bucketName: request.bucketName,
                    objectKey: request.objectKey,
                    resourceType: request.resourceType,
                    query: request.query,
                });
                return next(errors.MethodNotAllowed);
            }
            return metadataRoutes[request.method][request.resourceType](
                request, response, request.bucketName, request.objectKey, log,
                next);
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


module.exports = routeMetadata;
