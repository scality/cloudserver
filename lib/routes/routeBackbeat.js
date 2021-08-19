const url = require('url');
const async = require('async');

const { auth, errors, s3middleware } = require('arsenal');
const { responseJSONBody } = require('arsenal').s3routes.routesUtils;
const { getSubPartIds } = s3middleware.azureHelper.mpuUtils;
const vault = require('../auth/vault');
const data = require('../data/wrapper');
const metadata = require('../metadata/wrapper');
const locationConstraintCheck = require(
    '../api/apiUtils/object/locationConstraintCheck');
const { dataStore } = require('../api/apiUtils/object/storeObject');
const prepareRequestContexts = require(
'../api/apiUtils/authorization/prepareRequestContexts');
const { decodeVersionId } = require('../api/apiUtils/object/versioning');
const { metadataValidateBucketAndObj,
    metadataGetObject } = require('../metadata/metadataUtils');
const { BackendInfo } = require('../api/apiUtils/object/BackendInfo');
const { locationConstraints } = require('../Config').config;
const multipleBackendGateway = require('../data/multipleBackendGateway');
const constants = require('../../constants');
const { pushReplicationMetric } = require('./utilities/pushReplicationMetric');
const kms = require('../kms/wrapper');

auth.setHandler(vault);

const NAMESPACE = 'default';
const CIPHER = null; // replication/lifecycle does not work on encrypted objects

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
    return ['data', 'metadata', 'multiplebackenddata']
        .includes(req.resourceType);
}

function _respondWithHeaders(response, payload, extraHeaders, log, callback) {
    const body = typeof payload === 'object' ?
        JSON.stringify(payload) : payload;
    const httpHeaders = Object.assign({
        'x-amz-id-2': log.getSerializedUids(),
        'x-amz-request-id': log.getSerializedUids(),
        'content-type': 'application/json',
        'content-length': Buffer.byteLength(body),
    }, extraHeaders);
    response.writeHead(200, httpHeaders);
    response.end(body, 'utf8', () => {
        log.end().info('responded with payload', {
            httpCode: 200,
            contentLength: Buffer.byteLength(body),
        });
        callback();
    });
}

function _respond(response, payload, log, callback) {
    _respondWithHeaders(response, payload, {}, log, callback);
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
    if ((operation === 'initiatempu' || operation === 'putobject') &&
        headers['x-scal-version-id'] === undefined) {
        errMessage = 'bad request: missing x-scal-version-id header';
        log.error(errMessage);
        return errors.BadRequest.customizeDescription(errMessage);
    }
    if (operation === 'putpart' &&
        headers['x-scal-part-number'] === undefined) {
        errMessage = 'bad request: missing part-number header';
        log.error(errMessage);
        return errors.BadRequest.customizeDescription(errMessage);
    }
    if ((operation === 'putpart' || operation === 'completempu') &&
        headers['x-scal-upload-id'] === undefined) {
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
        if (headers['x-scal-source-version-id'] === undefined) {
            errMessage = 'bad request: missing x-scal-source-version-id header';
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
    const isValidLocation = location && storageType.includes(location.type);
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
    return multipleBackendGateway.objectTagging(type, request.objectKey,
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
    ?operation=deleteobjecttagging
POST /_/backbeat/multiplebackenddata/<bucket name>/<object key>
    ?operation=initiatempu
POST /_/backbeat/multiplebackenddata/<bucket name>/<object key>
    ?operation=completempu
POST /_/backbeat/multiplebackenddata/<bucket name>/<object key>
    ?operation=puttagging
POST /_/backbeat/batchdelete
*/

function createCipherBundle(bucketInfo, isV2Request, log, cb) {
    // Older backbeat versions do not support encryption (they ignore
    // encryption parameters returned), hence we shall not encrypt if
    // request comes from an older version of Backbeat
    if (isV2Request) {
        const serverSideEncryption = bucketInfo.getServerSideEncryption();
        if (serverSideEncryption) {
            return kms.createCipherBundle(serverSideEncryption, log, cb);
        }
    }
    return cb(null, null);
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
    const isV2Request = request.query.v2 === '';
    return createCipherBundle(bucketInfo, isV2Request, log, (err, cipherBundle) => {
        if (err) {
            log.error('error creating cipher bundle', {
                error: err,
                method: 'routeBackbeat',
            });
            return callback(errors.InternalError);
        }
        return dataStore(
            context, cipherBundle, request, payloadLen, {},
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
                if (cipherBundle) {
                    dataRetrievalInfo[0].cryptoScheme = cipherBundle.cryptoScheme;
                    dataRetrievalInfo[0].cipheredDataKey = cipherBundle.cipheredDataKey;
                    return _respondWithHeaders(response, dataRetrievalInfo, {
                        'x-amz-server-side-encryption': cipherBundle.algorithm,
                        'x-amz-server-side-encryption-aws-kms-key-id': cipherBundle.masterKeyId,
                    }, log, callback);
                }
                return _respond(response, dataRetrievalInfo, log, callback);
            });
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
            // use original data locations and encryption info
            [
                'location',
                'x-amz-server-side-encryption',
                'x-amz-server-side-encryption-aws-kms-key-id',
                'x-amz-server-side-encryption-customer-algorithm',
            ].forEach(headerName => {
                omVal[headerName] = objMd[headerName];
            });
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
    const metaHeaders = {
        'x-amz-meta-scal-replication-status': 'REPLICA',
        'x-amz-meta-scal-version-id': sourceVersionId,
    };
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
    const payloadLen = parseInt(request.headers['content-length'], 10);
    const backendInfo = new BackendInfo(storageLocation);
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
            const dataRetrievalInfo = {
                // TODO: Remove '' when versioning implemented for Azure.
                versionId: retrievalInfo.dataStoreVersionId || '',
            };
            return _respond(response, dataRetrievalInfo, log, callback);
        });
}

function deleteObject(request, response, log, callback) {
    const err = _checkMultipleBackendRequest(request, log);
    if (err) {
        return callback(err);
    }
    const storageLocation = request.headers['x-scal-storage-class'];
    const objectGetInfo = {
        key: request.objectKey,
        dataStoreName: storageLocation,
    };
    const reqUids = log.getSerializedUids();
    return multipleBackendGateway.delete(objectGetInfo, reqUids, err => {
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
    const metaHeaders = {
        'scal-replication-status': 'REPLICA',
        'scal-version-id': sourceVersionId,
    };
    if (userMetadata !== undefined) {
        try {
            const metaData = JSON.parse(userMetadata);
            Object.assign(metaHeaders, metaData);
        } catch (err) {
            // FIXME: add error type MalformedJSON
            return callback(errors.MalformedPOSTRequest);
        }
    }
    return multipleBackendGateway.createMPU(request.objectKey, metaHeaders,
        request.bucketName, undefined, storageLocation, contentType,
        cacheControl, contentDisposition, contentEncoding, null, log,
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

function putPart(request, response, log, callback) {
    const err = _checkMultipleBackendRequest(request, log);
    if (err) {
        return callback(err);
    }
    const storageLocation = request.headers['x-scal-storage-class'];
    const partNumber = request.headers['x-scal-part-number'];
    const uploadId = request.headers['x-scal-upload-id'];
    const payloadLen = parseInt(request.headers['content-length'], 10);
    return multipleBackendGateway.uploadPart(undefined, {}, request, payloadLen,
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
        const partList =
            getPartList(parts, request.objectKey, uploadId, storageLocation);
        // Azure client will set user metadata at this point.
        const metaHeaders = {
            'x-amz-meta-scal-replication-status': 'REPLICA',
            'x-amz-meta-scal-version-id': sourceVersionId,
        };
        if (userMetadata !== undefined) {
            try {
                const metaData = JSON.parse(userMetadata);
                Object.assign(metaHeaders, metaData);
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
        return multipleBackendGateway.completeMPU(request.objectKey, uploadId,
            storageLocation, partList, undefined, request.bucketName,
            metaHeaders, contentSettings, log, (err, retrievalInfo) => {
                if (err) {
                    log.error('error completing MPU', {
                        error: err,
                        method: 'completeMultipartUpload',
                    });
                    return callback(err);
                }
                const dataRetrievalInfo = {
                    // TODO: Remove '' when versioning implemented for Azure.
                    versionId: retrievalInfo.dataStoreVersionId || '',
                };
                return _respond(response, dataRetrievalInfo, log, callback);
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

function batchDelete(request, response, log, callback) {
    return _getRequestPayload(request, (err, payload) => {
        if (err) {
            return callback(err);
        }
        let request;
        try {
            request = JSON.parse(payload);
        } catch (e) {
            // FIXME: add error type MalformedJSON
            return callback(errors.MalformedPOSTRequest);
        }
        if (!request || !Array.isArray(request.Locations)) {
            return callback(errors.MalformedPOSTRequest);
        }
        const locations = request.Locations;
        log.trace('batch delete locations', { locations });
        return async.eachLimit(locations, 5, (loc, next) => {
            data.delete(loc, log, err => {
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
        },
    },
    GET: {
        metadata: getMetadata,
    },
};

function routeBackbeat(clientIP, request, response, log) {
    log.debug('routing request', {
        method: 'routeBackbeat',
        url: request.url,
    });
    _normalizeBackbeatRequest(request);
    const useMultipleBackend = request.resourceType === 'multiplebackenddata';
    const invalidRequest =
          (!request.resourceType ||
           (_isObjectRequest(request) &&
            (!request.bucketName || !request.objectKey)) ||
           (!request.query.operation && useMultipleBackend));
    const invalidRoute =
          (backbeatRoutes[request.method] === undefined ||
           backbeatRoutes[request.method][request.resourceType] === undefined ||
           (backbeatRoutes[request.method][request.resourceType]
            [request.query.operation] === undefined &&
            useMultipleBackend));
    log.addDefaultFields({
        bucketName: request.bucketName,
        objectKey: request.objectKey,
        bytesReceived: request.parsedContentLength || 0,
        bodyLength: parseInt(request.headers['content-length'], 10) || 0,
    });
    if (invalidRequest || invalidRoute) {
        log.debug(invalidRequest ? 'invalid request' : 'no such route', {
            method: request.method, bucketName: request.bucketName,
            objectKey: request.objectKey, resourceType: request.resourceType,
            query: request.query,
        });
        return responseJSONBody(errors.MethodNotAllowed, null, response, log);
    }

    if (!_isObjectRequest(request)) {
        const route = backbeatRoutes[request.method][request.resourceType];
        return route(request, response, log, err => {
            if (err) {
                return responseJSONBody(err, null, response, log);
            }
            return undefined;
        });
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
            const mdValParams = {
                bucketName: request.bucketName,
                objectKey: request.objectKey,
                authInfo: userInfo,
                versionId,
                requestType: 'ReplicateObject',
                request,
            };
            return metadataValidateBucketAndObj(mdValParams, log, next);
        },
        (bucketInfo, objMd, next) => {
            if (useMultipleBackend) {
                return backbeatRoutes[request.method][request.resourceType]
                    [request.query.operation](request, response, log, next);
            }
            const versioningConfig = bucketInfo.getVersioningConfiguration();
            if (!versioningConfig || versioningConfig.Status !== 'Enabled') {
                log.debug('bucket versioning is not enabled', {
                    method: request.method,
                    bucketName: request.bucketName,
                    objectKey: request.objectKey,
                    resourceType: request.resourceType,
                });
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
