const url = require('url');
const async = require('async');
const httpProxy = require('http-proxy');
const querystring = require('querystring');

const backbeatProxy = httpProxy.createProxyServer({
    ignorePath: true,
});
const { auth, errors, s3middleware, s3routes, models, storage } =
    require('arsenal');
const { responseJSONBody } = s3routes.routesUtils;
const { getSubPartIds } = s3middleware.azureHelper.mpuUtils;
const { skipMpuPartProcessing } = storage.data.external.backendUtils;
const { parseLC, MultipleBackendGateway } = storage.data;
const vault = require('../auth/vault');
const dataWrapper = require('../data/wrapper');
const metadata = require('../metadata/wrapper');
const locationConstraintCheck = require(
    '../api/apiUtils/object/locationConstraintCheck');
const locationStorageCheck =
    require('../api/apiUtils/object/locationStorageCheck');
const { dataStore } = require('../api/apiUtils/object/storeObject');
const prepareRequestContexts = require(
'../api/apiUtils/authorization/prepareRequestContexts');
const { decodeVersionId } = require('../api/apiUtils/object/versioning');
const { metadataValidateBucketAndObj,
    metadataGetObject } = require('../metadata/metadataUtils');
const { config } = require('../Config');
const constants = require('../../constants');
const { BackendInfo } = models;
const { pushReplicationMetric } = require('./utilities/pushReplicationMetric');

auth.setHandler(vault);

const NAMESPACE = 'default';
const CIPHER = null; // replication/lifecycle does not work on encrypted objects

let { locationConstraints } = config;
const { implName } = dataWrapper;
let dataClient = dataWrapper.client;
config.on('location-constraints-update', () => {
    locationConstraints = config.locationConstraints;
    if (implName === 'multipleBackends') {
        const clients = parseLC(config, vault);
        dataClient = new MultipleBackendGateway(
            clients, metadata, locationStorageCheck);
    }
});

function _decodeURI(uri) {
    // do the same decoding than in S3 server
    return decodeURIComponent(uri.replace(/\+/g, ' '));
}

function _normalizeBackbeatRequest(req) {
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

function _isObjectRequest(req) {
    return [
        'data',
        'metadata',
        'multiplebackenddata',
        'multiplebackendmetadata',
    ].includes(req.resourceType);
}

function _respond(response, payload, log, callback) {
    let body = '';
    if (typeof payload === 'string') {
        body = payload;
    } else if (typeof payload === 'object') {
        body = JSON.stringify(payload);
    }
    const httpHeaders = {
        'x-amz-id-2': log.getSerializedUids(),
        'x-amz-request-id': log.getSerializedUids(),
        'content-type': 'application/json',
        'content-length': Buffer.byteLength(body),
    };
    response.writeHead(200, httpHeaders);
    response.end(body, 'utf8', () => {
        log.end().info('responded with payload', {
            httpCode: 200,
            contentLength: Buffer.byteLength(body),
        });
        callback();
    });
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

function _checkMultipleBackendRequest(request, log) {
    const { headers, query } = request;
    const storageType = headers['x-scal-storage-type'];
    const { operation } = query;
    let errMessage;
    if (storageType === undefined) {
        errMessage = 'bad request: missing x-scal-storage-type header';
        log.error(errMessage);
        return errors.BadRequest.customizeDescription(errMessage);
    }
    if (operation === 'putpart' &&
        headers['x-scal-part-number'] === undefined) {
        errMessage = 'bad request: missing part-number header';
        log.error(errMessage);
        return errors.BadRequest.customizeDescription(errMessage);
    }
    const isMPUOperation =
        ['putpart', 'completempu', 'abortmpu'].includes(operation);
    if (isMPUOperation && headers['x-scal-upload-id'] === undefined) {
        errMessage = 'bad request: missing upload-id header';
        log.error(errMessage);
        return errors.BadRequest.customizeDescription(errMessage);
    }
    if (operation === 'putobject' &&
        headers['x-scal-canonical-id'] === undefined) {
        errMessage = 'bad request: missing x-scal-canonical-id header';
        log.error(errMessage);
        return errors.BadRequest.customizeDescription(errMessage);
    }
    // Ensure the external backend has versioning before asserting version ID.
    if (!constants.versioningNotImplBackends[storageType] &&
        (operation === 'puttagging' || operation === 'deletetagging')) {
        if (headers['x-scal-data-store-version-id'] === undefined) {
            errMessage =
                'bad request: missing x-scal-data-store-version-id header';
            log.error(errMessage);
            return errors.BadRequest.customizeDescription(errMessage);
        }
        if (headers['x-scal-source-bucket'] === undefined) {
            errMessage = 'bad request: missing x-scal-source-bucket header';
            log.error(errMessage);
            return errors.BadRequest.customizeDescription(errMessage);
        }
        if (headers['x-scal-replication-endpoint-site'] === undefined) {
            errMessage = 'bad request: missing ' +
                'x-scal-replication-endpoint-site';
            log.error(errMessage);
            return errors.BadRequest.customizeDescription(errMessage);
        }
    }
    if (operation === 'putobject' &&
        headers['content-md5'] === undefined) {
        errMessage = 'bad request: missing content-md5 header';
        log.error(errMessage);
        return errors.BadRequest.customizeDescription(errMessage);
    }
    if (headers['x-scal-storage-class'] === undefined) {
        errMessage = 'bad request: missing x-scal-storage-class header';
        log.error(errMessage);
        return errors.BadRequest.customizeDescription(errMessage);
    }
    const location = locationConstraints[headers['x-scal-storage-class']];
    const storageTypeList = storageType.split(',');
    const isValidLocation = location &&
          storageTypeList.includes(location.type);
    if (!isValidLocation) {
        errMessage = 'invalid request: invalid location constraint in request';
        log.debug(errMessage, {
            method: request.method,
            bucketName: request.bucketName,
            objectKey: request.objectKey,
            resourceType: request.resourceType,
        });
        return errors.InvalidRequest.customizeDescription(errMessage);
    }
    return undefined;
}

function getPartList(parts, objectKey, uploadId, storageLocation) {
    const partList = {};
    if (locationConstraints[storageLocation].type === 'azure') {
        partList.uncommittedBlocks = [];
        parts.forEach(part => {
            const location = {
                key: objectKey,
                partNumber: part.PartNumber[0],
                dataStoreETag: part.ETag[0],
                numberSubParts: part.NumberSubParts[0],
            };
            const subPartIds = getSubPartIds(location, uploadId);
            partList.uncommittedBlocks.push(...subPartIds);
        });
    } else {
        partList.Part = parts;
    }

    return partList;
}

function generateMpuAggregateInfo(parts) {
    let aggregateSize;

    // CopyLocationTask does transmit a size for each part,
    // MultipleBackendTask does not, so check if size is defined in
    // the first part.
    if (parts[0] && parts[0].Size) {
        aggregateSize = parts.reduce(
            (agg, part) => agg + Number.parseInt(part.Size[0], 10), 0);
    }
    return {
        aggregateSize,
        aggregateETag: s3middleware.processMpuParts.createAggregateETag(
            parts.map(part => part.ETag[0])),
    };
}

/**
 * Helper to create the response object for putObject and completeMPU
 *
 * @param {object} params - response info
 * @param {string} params.dataStoreName - name of location
 * @param {string} params.dataStoreType - location type (e.g. "aws_s3")
 * @param {string} params.key - object key
 * @param {number} params.size - total byte length
 * @param {string} params.dataStoreETag - object ETag
 * @param {string} [params.dataStoreVersionId] - object version ID, if
 * versioned
 * @return {object} - the response object to serialize and send back
 */
function constructPutResponse(params) {
    // FIXME: The main data locations array may eventually resemble
    // locations stored in replication info object, i.e. without
    // size/start for cloud locations, which could ease passing
    // standard location objects across services. For now let's just
    // create the location as they are usually stored in the
    // "locations" attribute, with size/start info.

    const location = [{
        dataStoreName: params.dataStoreName,
        dataStoreType: params.dataStoreType,
        key: params.key,
        start: 0,
        size: params.size,
        dataStoreETag: params.dataStoreETag,
        dataStoreVersionId: params.dataStoreVersionId,
    }];
    return {
        // TODO: Remove '' when versioning implemented for Azure.
        versionId: params.dataStoreVersionId || '',
        location,
    };
}

function handleTaggingOperation(request, response, type, dataStoreVersionId,
    log, callback) {
    const storageLocation = request.headers['x-scal-storage-class'];
    const objectMD = {
        dataStoreName: storageLocation,
        location: [{ dataStoreVersionId }],
    };
    if (type === 'Put') {
        try {
            const tags = JSON.parse(request.headers['x-scal-tags']);
            objectMD.tags = tags;
        } catch (err) {
            // FIXME: add error type MalformedJSON
            return callback(errors.MalformedPOSTRequest);
        }
    }
    return dataClient.objectTagging(type, request.objectKey,
    request.bucketName, objectMD, log, err => {
        if (err) {
            log.error(`error during object tagging: ${type}`, {
                error: err,
                method: 'handleTaggingOperation',
            });
            return callback(err);
        }
        const dataRetrievalInfo = {
            versionId: dataStoreVersionId,
        };
        return _respond(response, dataRetrievalInfo, log, callback);
    });
}

/*
PUT /_/backbeat/metadata/<bucket name>/<object key>
GET /_/backbeat/metadata/<bucket name>/<object key>?versionId=<version id>
PUT /_/backbeat/data/<bucket name>/<object key>
PUT /_/backbeat/multiplebackenddata/<bucket name>/<object key>
    ?operation=putobject
PUT /_/backbeat/multiplebackenddata/<bucket name>/<object key>
    ?operation=putpart
DELETE /_/backbeat/multiplebackenddata/<bucket name>/<object key>
    ?operation=deleteobject
DELETE /_/backbeat/multiplebackenddata/<bucket name>/<object key>
    ?operation=abortmpu
DELETE /_/backbeat/multiplebackenddata/<bucket name>/<object key>
    ?operation=deleteobjecttagging
POST /_/backbeat/multiplebackenddata/<bucket name>/<object key>
    ?operation=initiatempu
POST /_/backbeat/multiplebackenddata/<bucket name>/<object key>
    ?operation=completempu
POST /_/backbeat/multiplebackenddata/<bucket name>/<object key>
    ?operation=puttagging
GET /_/backbeat/multiplebackendmetadata/<bucket name>/<object key>
POST /_/backbeat/batchdelete
*/

function _getLastModified(locations, log, cb) {
    const reqUids = log.getSerializedUids();
    return dataClient.head(locations, reqUids, (err, data) => {
        if (err) {
            log.error('head object request failed', {
                method: 'headObject',
                error: err,
            });
            return cb(err);
        }
        return cb(null, data.LastModified || data.lastModified);
    });
}

function headObject(request, response, log, cb) {
    let locations;
    try {
        locations = JSON.parse(request.headers['x-scal-locations']);
    } catch (e) {
        const msg = 'x-scal-locations header is invalid';
        return cb(errors.InvalidRequest.customizeDescription(msg));
    }
    if (!Array.isArray(locations)) {
        const msg = 'x-scal-locations header is invalid';
        return cb(errors.InvalidRequest.customizeDescription(msg));
    }
    return _getLastModified(locations, log, (err, lastModified) => {
        if (err) {
            return cb(err);
        }
        const dataRetrievalInfo = { lastModified };
        return _respond(response, dataRetrievalInfo, log, cb);
    });
}

function putData(request, response, bucketInfo, objMd, log, callback) {
    let errMessage;
    const canonicalID = request.headers['x-scal-canonical-id'];
    if (canonicalID === undefined) {
        errMessage = 'bad request: missing x-scal-canonical-id header';
        log.error(errMessage);
        return callback(errors.BadRequest.customizeDescription(errMessage));
    }
    const contentMD5 = request.headers['content-md5'];
    if (contentMD5 === undefined) {
        errMessage = 'bad request: missing content-md5 header';
        log.error(errMessage);
        return callback(errors.BadRequest.customizeDescription(errMessage));
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
                log.error('error putting data', {
                    error: err,
                    method: 'putData',
                });
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
            log.info('successfully put data', {
                method: 'routeBackbeat:putData',
            });
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
            versioning: bucketInfo.isVersioningEnabled(),
            versionId: omVal.versionId,
        };
        // If the object is from a source bucket without versioning (i.e. NFS),
        // then we want to create a version for the replica object even though
        // none was provided in the object metadata value.
        if (omVal.replicationInfo.isNFS) {
            const isReplica = omVal.replicationInfo.status === 'REPLICA';
            options.versioning = isReplica;
            omVal.replicationInfo.isNFS = !isReplica;
        }
        log.trace('putting object version', {
            objectKey: request.objectKey, omVal, options });
        return metadata.putObjectMD(bucketName, objectKey, omVal, options, log,
            (err, md) => {
                if (err) {
                    log.error('error putting object metadata', {
                        error: err,
                        method: 'putMetadata',
                    });
                    return callback(err);
                }
                pushReplicationMetric(objMd, omVal, bucketName, objectKey, log);
                return _respond(response, md, log, callback);
            });
    });
}

function putObject(request, response, log, callback) {
    const err = _checkMultipleBackendRequest(request, log);
    if (err) {
        return callback(err);
    }
    const storageLocation = request.headers['x-scal-storage-class'];
    const sourceVersionId = request.headers['x-scal-version-id'];
    const canonicalID = request.headers['x-scal-canonical-id'];
    const contentMD5 = request.headers['content-md5'];
    const contentType = request.headers['x-scal-content-type'];
    const userMetadata = request.headers['x-scal-user-metadata'];
    const cacheControl = request.headers['x-scal-cache-control'];
    const contentDisposition = request.headers['x-scal-content-disposition'];
    const contentEncoding = request.headers['x-scal-content-encoding'];
    const tagging = request.headers['x-scal-tags'];
    const metaHeaders = { 'x-amz-meta-scal-replication-status': 'REPLICA' };
    if (sourceVersionId) {
        metaHeaders['x-amz-meta-scal-version-id'] = sourceVersionId;
    }
    if (userMetadata !== undefined) {
        try {
            const metaData = JSON.parse(userMetadata);
            Object.assign(metaHeaders, metaData);
        } catch (err) {
            // FIXME: add error type MalformedJSON
            return callback(errors.MalformedPOSTRequest);
        }
    }
    const context = {
        bucketName: request.bucketName,
        owner: canonicalID,
        namespace: NAMESPACE,
        objectKey: request.objectKey,
        metaHeaders,
        contentType,
        cacheControl,
        contentDisposition,
        contentEncoding,
    };
    if (tagging !== undefined) {
        try {
            const tags = JSON.parse(request.headers['x-scal-tags']);
            context.tagging = querystring.stringify(tags);
        } catch (err) {
            // FIXME: add error type MalformedJSON
            return callback(errors.MalformedPOSTRequest);
        }
    }
    const payloadLen = parseInt(request.headers['content-length'], 10);
    const backendInfo = new BackendInfo(config, storageLocation);
    return dataStore(context, CIPHER, request, payloadLen, {}, backendInfo, log,
        (err, retrievalInfo, md5) => {
            if (err) {
                log.error('error putting data', {
                    error: err,
                    method: 'putObject',
                });
                return callback(err);
            }
            if (contentMD5 !== md5) {
                return callback(errors.BadDigest);
            }
            const responsePayload = constructPutResponse({
                dataStoreName: retrievalInfo.dataStoreName,
                dataStoreType: retrievalInfo.dataStoreType,
                key: retrievalInfo.key,
                size: payloadLen,
                dataStoreETag: retrievalInfo.dataStoreETag ?
                    `1:${retrievalInfo.dataStoreETag}` : `1:${md5}`,
                dataStoreVersionId: retrievalInfo.dataStoreVersionId,
            });
            return _respond(response, responsePayload, log, callback);
        });
}

function deleteObject(request, response, log, callback) {
    const err = _checkMultipleBackendRequest(request, log);
    if (err) {
        return callback(err);
    }
    const storageLocation = request.headers['x-scal-storage-class'];
    const objectGetInfo = dataClient.toObjectGetInfo(
        request.objectKey, request.bucketName, storageLocation);
    if (!objectGetInfo) {
        log.error('error deleting object in multiple backend', {
            error: 'cannot create objectGetInfo',
            method: 'deleteObject',
        });
        return callback(errors.InternalError);
    }
    const reqUids = log.getSerializedUids();
    return dataClient.delete(objectGetInfo, reqUids, err => {
        if (err) {
            log.error('error deleting object in multiple backend', {
                error: err,
                method: 'deleteObject',
            });
            return callback(err);
        }
        return _respond(response, {}, log, callback);
    });
}

function getMetadata(request, response, bucketInfo, objectMd, log, cb) {
    if (!objectMd) {
        return cb(errors.ObjNotFound);
    }
    return _respond(response, { Body: JSON.stringify(objectMd) }, log, cb);
}

function initiateMultipartUpload(request, response, log, callback) {
    const err = _checkMultipleBackendRequest(request, log);
    if (err) {
        return callback(err);
    }
    const storageLocation = request.headers['x-scal-storage-class'];
    const sourceVersionId = request.headers['x-scal-version-id'];
    const contentType = request.headers['x-scal-content-type'];
    const userMetadata = request.headers['x-scal-user-metadata'];
    const cacheControl = request.headers['x-scal-cache-control'];
    const contentDisposition = request.headers['x-scal-content-disposition'];
    const contentEncoding = request.headers['x-scal-content-encoding'];
    const tags = request.headers['x-scal-tags'];
    const metaHeaders = { 'x-amz-meta-scal-replication-status': 'REPLICA' };
    if (sourceVersionId) {
        metaHeaders['x-amz-meta-scal-version-id'] = sourceVersionId;
    }
    if (userMetadata !== undefined) {
        try {
            const metaData = JSON.parse(userMetadata);
            Object.assign(metaHeaders, metaData);
        } catch (err) {
            // FIXME: add error type MalformedJSON
            return callback(errors.MalformedPOSTRequest);
        }
    }
    let tagging;
    if (tags !== undefined) {
        try {
            const parsedTags = JSON.parse(request.headers['x-scal-tags']);
            tagging = querystring.stringify(parsedTags);
        } catch (err) {
            // FIXME: add error type MalformedJSON
            return callback(errors.MalformedPOSTRequest);
        }
    }
    return dataClient.createMPU(request.objectKey, metaHeaders,
        request.bucketName, undefined, storageLocation, contentType,
        cacheControl, contentDisposition, contentEncoding, tagging, log,
        (err, data) => {
            if (err) {
                log.error('error initiating multipart upload', {
                    error: err,
                    method: 'initiateMultipartUpload',
                });
                return callback(err);
            }
            const dataRetrievalInfo = {
                uploadId: data.UploadId,
            };
            return _respond(response, dataRetrievalInfo, log, callback);
        });
}

function abortMultipartUpload(request, response, log, callback) {
    const err = _checkMultipleBackendRequest(request, log);
    if (err) {
        return callback(err);
    }
    const storageLocation = request.headers['x-scal-storage-class'];
    const uploadId = request.headers['x-scal-upload-id'];
    return dataClient.abortMPU(request.objectKey, uploadId,
        storageLocation, request.bucketName, log, err => {
            if (err) {
                log.error('error aborting MPU', {
                    error: err,
                    method: 'abortMultipartUpload',
                });
                return callback(err);
            }
            return _respond(response, {}, log, callback);
        });
}

function putPart(request, response, log, callback) {
    const err = _checkMultipleBackendRequest(request, log);
    if (err) {
        return callback(err);
    }
    const storageLocation = request.headers['x-scal-storage-class'];
    const partNumber = request.headers['x-scal-part-number'];
    const uploadId = request.headers['x-scal-upload-id'];
    const payloadLen = parseInt(request.headers['content-length'], 10);
    return dataClient.uploadPart(undefined, {}, request, payloadLen,
        storageLocation, request.objectKey, uploadId, partNumber,
        request.bucketName, log, (err, data) => {
            if (err) {
                log.error('error putting MPU part', {
                    error: err,
                    method: 'putPart',
                });
                return callback(err);
            }
            const dataRetrievalInfo = {
                partNumber,
                ETag: data.dataStoreETag,
                numberSubParts: data.numberSubParts,
            };
            return _respond(response, dataRetrievalInfo, log, callback);
        });
}

function completeMultipartUpload(request, response, log, callback) {
    const err = _checkMultipleBackendRequest(request, log);
    if (err) {
        return callback(err);
    }
    const storageLocation = request.headers['x-scal-storage-class'];
    const sourceVersionId = request.headers['x-scal-version-id'];
    const uploadId = request.headers['x-scal-upload-id'];
    const userMetadata = request.headers['x-scal-user-metadata'];
    const contentType = request.headers['x-scal-content-type'];
    const cacheControl = request.headers['x-scal-cache-control'];
    const contentDisposition = request.headers['x-scal-content-disposition'];
    const contentEncoding = request.headers['x-scal-content-encoding'];
    const tags = request.headers['x-scal-tags'];
    const data = [];
    let totalLength = 0;
    request.on('data', chunk => {
        totalLength += chunk.length;
        data.push(chunk);
    });
    request.on('end', () => {
        let parts;
        try {
            parts = JSON.parse(Buffer.concat(data), totalLength);
        } catch (e) {
            // FIXME: add error type MalformedJSON
            return callback(errors.MalformedPOSTRequest);
        }
        const partList = getPartList(
            parts, request.objectKey, uploadId, storageLocation);
        // Azure client will set user metadata at this point.
        const metaHeaders = { 'x-amz-meta-scal-replication-status': 'REPLICA' };
        if (sourceVersionId) {
            metaHeaders['x-amz-meta-scal-version-id'] = sourceVersionId;
        }
        if (userMetadata !== undefined) {
            try {
                const metaData = JSON.parse(userMetadata);
                Object.assign(metaHeaders, metaData);
            } catch (err) {
                // FIXME: add error type MalformedJSON
                return callback(errors.MalformedPOSTRequest);
            }
        }
        // Azure does not have a notion of initiating an MPU, so we put any
        // tagging fields during the complete MPU if using Azure.
        let tagging;
        if (tags !== undefined) {
            try {
                const parsedTags = JSON.parse(request.headers['x-scal-tags']);
                tagging = querystring.stringify(parsedTags);
            } catch (err) {
                // FIXME: add error type MalformedJSON
                return callback(errors.MalformedPOSTRequest);
            }
        }
        const contentSettings = {
            contentType: contentType || undefined,
            cacheControl: cacheControl || undefined,
            contentDisposition: contentDisposition || undefined,
            contentEncoding: contentEncoding || undefined,
        };
        return dataClient.completeMPU(request.objectKey, uploadId,
            storageLocation, partList, undefined, request.bucketName,
            metaHeaders, contentSettings, tagging, log,
            (err, retrievalInfo) => {
                if (err) {
                    log.error('error completing MPU', {
                        error: err,
                        method: 'completeMultipartUpload',
                    });
                    return callback(err);
                }
                // The logic here is an aggregate of code coming from
                // lib/api/completeMultipartUpload.js.

                const { key, dataStoreType, dataStoreVersionId } =
                      retrievalInfo;
                let size;
                let dataStoreETag;
                if (skipMpuPartProcessing(retrievalInfo)) {
                    size = retrievalInfo.contentLength;
                    dataStoreETag = retrievalInfo.eTag;
                } else {
                    const { aggregateSize, aggregateETag } =
                          generateMpuAggregateInfo(parts);
                    size = aggregateSize;
                    dataStoreETag = aggregateETag;
                }
                const responsePayload = constructPutResponse({
                    dataStoreName: storageLocation,
                    dataStoreType,
                    key,
                    size,
                    dataStoreETag,
                    dataStoreVersionId,
                });
                return _respond(response, responsePayload, log, callback);
            });
    });
    return undefined;
}

function putObjectTagging(request, response, log, callback) {
    const err = _checkMultipleBackendRequest(request, log);
    if (err) {
        return callback(err);
    }
    const sourceVersionId = request.headers['x-scal-source-version-id'];
    const sourceBucket = request.headers['x-scal-source-bucket'];
    const site = request.headers['x-scal-replication-endpoint-site'];
    let dataStoreVersionId = request.headers['x-scal-data-store-version-id'];
    // If the tagging request is made before the replication has completed, the
    // Kafka entry will not have the dataStoreVersionId available so we
    // retrieve it from metadata here.
    if (dataStoreVersionId === '') {
        return metadataGetObject(sourceBucket, request.objectKey,
            sourceVersionId, log, (err, objMD) => {
                if (err) {
                    return callback(err);
                }
                const backend = objMD.replicationInfo.backends.find(o =>
                    o.site === site);
                dataStoreVersionId = backend.dataStoreVersionId;
                return handleTaggingOperation(request, response, 'Put',
                    dataStoreVersionId, log, callback);
            });
    }
    return handleTaggingOperation(request, response, 'Put', dataStoreVersionId,
        log, callback);
}

function deleteObjectTagging(request, response, log, callback) {
    const err = _checkMultipleBackendRequest(request, log);
    if (err) {
        return callback(err);
    }
    const sourceVersionId = request.headers['x-scal-source-version-id'];
    const sourceBucket = request.headers['x-scal-source-bucket'];
    const site = request.headers['x-scal-replication-endpoint-site'];
    let dataStoreVersionId = request.headers['x-scal-data-store-version-id'];
    // If the tagging request is made before the replication has completed, the
    // Kafka entry will not have the dataStoreVersionId available so we
    // retrieve it from metadata here.
    if (dataStoreVersionId === '') {
        return metadataGetObject(sourceBucket, request.objectKey,
            sourceVersionId, log, (err, objMD) => {
                if (err) {
                    return callback(err);
                }
                const backend = objMD.replicationInfo.backends.find(o =>
                    o.site === site);
                dataStoreVersionId = backend.dataStoreVersionId;
                return handleTaggingOperation(request, response, 'Delete',
                    dataStoreVersionId, log, callback);
            });
    }
    return handleTaggingOperation(request, response, 'Delete',
        dataStoreVersionId, log, callback);
}

function _createAzureConditionalDeleteObjectGetInfo(request) {
    const { objectKey, bucketName, headers } = request;
    const objectGetInfo = dataClient.toObjectGetInfo(
        objectKey, bucketName, headers['x-scal-storage-class']);
    return Object.assign({}, objectGetInfo, {
        options: {
            accessConditions: {
                DateUnModifiedSince: new Date(headers['if-unmodified-since']),
            },
        },
    });
}

function _azureConditionalDelete(request, response, log, cb) {
    const objectGetInfo = _createAzureConditionalDeleteObjectGetInfo(request);
    const reqUids = log.getSerializedUids();
    return dataClient.delete(objectGetInfo, reqUids, err => {
        if (err && err.code === 412) {
            log.info('precondition for Azure deletion was not met', {
                method: '_azureConditionalDelete',
                key: request.objectKey,
                bucket: request.bucketName,
            });
            return cb(err);
        }
        if (err) {
            log.error('error deleting object in Azure', {
                error: err,
                method: '_azureConditionalDelete',
            });
            return cb(err);
        }
        return _respond(response, {}, log, cb);
    });
}

function _putTagging(request, response, log, cb) {
    return handleTaggingOperation(
        request, response, 'Put', undefined, log, err => {
            if (err) {
                log.error('put tagging failed', {
                    method: '_putTagging',
                    error: err,
                });
                return cb(err);
            }
            return _respond(response, null, log, cb);
        });
}

function _conditionalTagging(request, response, locations, log, cb) {
    return _getLastModified(locations, log, (err, lastModified) => {
        if (err) {
            return cb(err);
        }
        const ifUnmodifiedSince = request.headers['if-unmodified-since'];
        if (new Date(ifUnmodifiedSince) < new Date(lastModified)) {
            log.info('object has been modified, skipping tagging operation', {
                method: '_conditionalTagging',
                ifUnmodifiedSince,
                lastModified,
                key: request.objectKey,
                bucket: request.bucketName,
            });
            return _respond(response, null, log, cb);
        }
        return _putTagging(request, response, log, cb);
    });
}

function _performConditionalDelete(request, response, locations, log, cb) {
    const { headers } = request;
    const location = locationConstraints[headers['x-scal-storage-class']];
    if (!request.headers['if-unmodified-since']) {
        log.info('unknown last modified time, skipping conditional delete', {
            method: '_performConditionalDelete',
        });
        return _respond(response, null, log, cb);
    }
    // Azure supports a conditional delete operation.
    if (location && location.type === 'azure') {
        return _azureConditionalDelete(request, response, log, cb);
    }
    // Other clouds do not support a conditional delete. Instead, we
    // conditionally put tags to indicate if it should be deleted by the user.
    return _conditionalTagging(request, response, locations, log, cb);
}

function _shouldConditionallyDelete(request, locations) {
    if (locations.length === 0) {
        return false;
    }
    const storageClass = request.headers['x-scal-storage-class'];
    const type =
        storageClass &&
        locationConstraints[storageClass] &&
        locationConstraints[storageClass].type;
    const isExternalBackend = type && constants.externalBackends[type];
    const isNotVersioned = !locations[0].dataStoreVersionId;
    return isExternalBackend && isNotVersioned;
}

function batchDelete(request, response, log, callback) {
    return _getRequestPayload(request, (err, payload) => {
        if (err) {
            return callback(err);
        }
        let parsedPayload;
        try {
            parsedPayload = JSON.parse(payload);
        } catch (e) {
            // FIXME: add error type MalformedJSON
            return callback(errors.MalformedPOSTRequest);
        }
        if (!parsedPayload || !Array.isArray(parsedPayload.Locations)) {
            return callback(errors.MalformedPOSTRequest);
        }
        const locations = parsedPayload.Locations;
        if (_shouldConditionallyDelete(request, locations)) {
            return _performConditionalDelete(
                request, response, locations, log, callback);
        }
        log.trace('batch delete locations', { locations });
        return async.eachLimit(locations, 5, (loc, next) => {
            const _loc = Object.assign({}, loc);
            if (_loc.dataStoreVersionId !== undefined) {
                // required by cloud backends
                _loc.deleteVersion = true;
            }
            dataWrapper.data.delete(_loc, log, err => {
                if (err && err.ObjNotFound) {
                    log.info('batch delete: data location do not exist', {
                        method: 'batchDelete',
                        location: loc,
                    });
                    return next();
                }
                return next(err);
            });
        }, err => {
            if (err) {
                log.error('batch delete failed', {
                    method: 'batchDelete',
                    locations,
                    error: err,
                });
                return callback(err);
            }
            log.debug('batch delete successful', { locations });
            return _respond(response, null, log, callback);
        });
    });
}

const backbeatRoutes = {
    PUT: {
        data: putData,
        metadata: putMetadata,
        multiplebackenddata: {
            putobject: putObject,
            putpart: putPart,
        },
    },
    POST: {
        multiplebackenddata: {
            initiatempu: initiateMultipartUpload,
            completempu: completeMultipartUpload,
            puttagging: putObjectTagging,
        },
        batchdelete: batchDelete,
    },
    DELETE: {
        multiplebackenddata: {
            deleteobject: deleteObject,
            deleteobjecttagging: deleteObjectTagging,
            abortmpu: abortMultipartUpload,
        },
    },
    GET: {
        metadata: getMetadata,
        multiplebackendmetadata: headObject,
    },
};

function routeBackbeat(clientIP, request, response, log) {
    log.debug('routing request', {
        method: 'routeBackbeat',
        url: request.url,
    });
    _normalizeBackbeatRequest(request);
    const requestContexts = prepareRequestContexts('objectReplicate', request);

    // proxy api requests to Backbeat API server
    if (request.resourceType === 'api') {
        if (!config.backbeat) {
            log.debug('unable to proxy backbeat api request', {
                backbeatConfig: config.backbeat,
            });
            return responseJSONBody(errors.MethodNotAllowed, null, response,
                log);
        }
        const path = request.url.replace('/_/backbeat/api', '/_/');
        const { host, port } = config.backbeat;
        const target = `http://${host}:${port}${path}`;
        return auth.server.doAuth(request, log, (err, userInfo) => {
            if (err) {
                log.debug('authentication error', {
                    error: err,
                    method: request.method,
                    bucketName: request.bucketName,
                    objectKey: request.objectKey,
                });
                return responseJSONBody(err, null, response, log);
            }
            // FIXME for now, any authenticated user can access API
            // routes. We should introduce admin accounts or accounts
            // with admin privileges, and restrict access to those
            // only.
            if (userInfo.getCanonicalID() === constants.publicId) {
                log.debug('unauthenticated access to API routes', {
                    method: request.method,
                    bucketName: request.bucketName,
                    objectKey: request.objectKey,
                });
                return responseJSONBody(
                    errors.AccessDenied, null, response, log);
            }
            return backbeatProxy.web(request, response, { target }, err => {
                log.error('error proxying request to api server',
                          { error: err.message });
                return responseJSONBody(errors.ServiceUnavailable, null,
                                        response, log);
            });
        }, 's3', requestContexts);
    }

    const useMultipleBackend =
        request.resourceType.startsWith('multiplebackend');
    const invalidRequest =
          (!request.resourceType ||
           (_isObjectRequest(request) &&
            (!request.bucketName || !request.objectKey)) ||
           (!request.query.operation &&
             request.resourceType === 'multiplebackenddata'));
    const invalidRoute =
          (backbeatRoutes[request.method] === undefined ||
           backbeatRoutes[request.method][request.resourceType] === undefined ||
           (backbeatRoutes[request.method][request.resourceType]
            [request.query.operation] === undefined &&
            request.resourceType === 'multiplebackenddata'));
    if (invalidRequest || invalidRoute) {
        log.debug(invalidRequest ? 'invalid request' : 'no such route', {
            method: request.method, bucketName: request.bucketName,
            objectKey: request.objectKey, resourceType: request.resourceType,
            query: request.query,
        });
        return responseJSONBody(errors.MethodNotAllowed, null, response, log);
    }

    log.addDefaultFields({
        bucketName: request.bucketName,
        objectKey: request.objectKey,
        bytesReceived: request.parsedContentLength || 0,
        bodyLength: parseInt(request.headers['content-length'], 10) || 0,
    });

    if (!_isObjectRequest(request)) {
        const route = backbeatRoutes[request.method][request.resourceType];
        return route(request, response, log, err => {
            if (err) {
                return responseJSONBody(err, null, response, log);
            }
            return undefined;
        });
    }
    const decodedVidResult = decodeVersionId(request.query);
    if (decodedVidResult instanceof Error) {
        log.trace('invalid versionId query', {
            versionId: request.query.versionId,
            error: decodedVidResult,
        });
        return responseJSONBody(errors.InvalidArgument, null, response, log);
    }
    const versionId = decodedVidResult;
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
            if (useMultipleBackend) {
                // Bucket and object do not exist in metadata.
                return next(null, null, null);
            }
            const mdValParams = { bucketName: request.bucketName,
                objectKey: request.objectKey,
                authInfo: userInfo,
                versionId,
                requestType: 'ReplicateObject' };
            return metadataValidateBucketAndObj(mdValParams, log, next);
        },
        (bucketInfo, objMd, next) => {
            if (!useMultipleBackend) {
                return backbeatRoutes[request.method][request.resourceType](
                    request, response, bucketInfo, objMd, log, next);
            }
            if (request.resourceType === 'multiplebackendmetadata') {
                return backbeatRoutes[request.method][request.resourceType](
                    request, response, log, next);
            }
            return backbeatRoutes[request.method][request.resourceType]
                [request.query.operation](request, response, log, next);
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
