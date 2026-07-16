const {
    constants: { HTTP_STATUS_CONFLICT },
} = require('http2');
const url = require('url');
const async = require('async');
const httpProxy = require('http-proxy');
const querystring = require('querystring');
const joi = require('@hapi/joi');

const backbeatProxy = httpProxy.createProxyServer({
    ignorePath: true,
});
const { auth, errors, errorInstances, s3middleware, s3routes, models, storage, versioning } = require('arsenal');
const { decode, encode } = versioning.VersionID;
const {
    VersionIdCollisionException,
    StaleMicroVersionIdException,
    MicroVersionIdAlreadyStoredException,
} = require('@scality/cloudserverclient');

const { responseJSONBody } = s3routes.routesUtils;
const { getSubPartIds } = s3middleware.azureHelper.mpuUtils;
const { skipMpuPartProcessing } = storage.data.external.backendUtils;
const { parseLC, MultipleBackendGateway } = storage.data;
const vault = require('../auth/vault');
const dataWrapper = require('../data/wrapper');
const metadata = require('../metadata/wrapper');
const locationConstraintCheck = require('../api/apiUtils/object/locationConstraintCheck');
const locationStorageCheck = require('../api/apiUtils/object/locationStorageCheck');
const { dataStore } = require('../api/apiUtils/object/storeObject');
const prepareRequestContexts = require('../api/apiUtils/authorization/prepareRequestContexts');
const { decodeVersionId } = require('../api/apiUtils/object/versioning');
const getReplicationInfo = require('../api/apiUtils/object/getReplicationInfo');
const locationKeysHaveChanged = require('../api/apiUtils/object/locationKeysHaveChanged');
const { standardMetadataValidateBucketAndObj, metadataGetObject } = require('../metadata/metadataUtils');
const { config } = require('../Config');
const constants = require('../../constants');
const {
    defaultChecksumData,
    getChecksumDataFromHeaders,
    arsenalErrorFromChecksumError,
} = require('../api/apiUtils/integrity/validateChecksums');
const { BackendInfo } = models;
const { pushReplicationMetric } = require('./utilities/pushReplicationMetric');
const writeContinue = require('../utilities/writeContinue');
const kms = require('../kms/wrapper');
const { listLifecycleCurrents } = require('../api/backbeat/listLifecycleCurrents');
const { listLifecycleNonCurrents } = require('../api/backbeat/listLifecycleNonCurrents');
const { listLifecycleOrphanDeleteMarkers } = require('../api/backbeat/listLifecycleOrphanDeleteMarkers');
const { objectDeleteInternal } = require('../api/objectDelete');
const quotaUtils = require('../api/apiUtils/quotas/quotaUtils');
const { handleAuthorizationResults } = require('../api/api');
const { versioningPreprocessing } = require('../api/apiUtils/object/versioning');
const { promisify } = require('util');

const versioningPreprocessingPromised = promisify(versioningPreprocessing);
metadata.getObjectMDPromised = promisify(metadata.getObjectMD);
metadata.getBucketAndObjectMDPromised = promisify(metadata.getBucketAndObjectMD);

const { CURRENT_TYPE, NON_CURRENT_TYPE, ORPHAN_DM_TYPE } = constants.lifecycleListing;

const lifecycleTypeCalls = {
    [CURRENT_TYPE]: listLifecycleCurrents,
    [NON_CURRENT_TYPE]: listLifecycleNonCurrents,
    [ORPHAN_DM_TYPE]: listLifecycleOrphanDeleteMarkers,
};

auth.setHandler(vault);

const NAMESPACE = 'default';
const CIPHER = null; // replication/lifecycle does not work on encrypted objects

let { locationConstraints } = config;
const { nullVersionCompatMode } = config;
const { implName } = dataWrapper;
let dataClient = dataWrapper.client;
config.on('location-constraints-update', () => {
    locationConstraints = config.locationConstraints;
    if (implName === 'multipleBackends') {
        const clients = parseLC(config, vault);
        dataClient = new MultipleBackendGateway(clients, metadata, locationStorageCheck);
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
    req.actionImplicitDenies = false;
    /* eslint-enable no-param-reassign */
}

function _isObjectRequest(req) {
    return ['data', 'metadata', 'multiplebackenddata', 'multiplebackendmetadata'].includes(req.resourceType);
}

function _respondWithHeaders(response, payload, extraHeaders, log, callback, statusCode = 200) {
    let body = '';
    if (typeof payload === 'string') {
        body = payload;
    } else if (typeof payload === 'object') {
        body = JSON.stringify(payload);
    }
    const httpHeaders = Object.assign(
        {
            'x-amz-id-2': log.getSerializedUids(),
            'x-amz-request-id': log.getSerializedUids(),
            'content-type': 'application/json',
            'content-length': Buffer.byteLength(body),
        },
        extraHeaders,
    );
    if (response.serverAccessLog) {
        // eslint-disable-next-line no-param-reassign
        response.serverAccessLog.bytesSent = Buffer.byteLength(body);
        // eslint-disable-next-line no-param-reassign
        response.serverAccessLog.endTurnAroundTime = process.hrtime.bigint();
    }
    response.writeHead(statusCode, httpHeaders);
    response.end(body, 'utf8', () => {
        log.end().info('responded with payload', {
            httpCode: statusCode,
            contentLength: Buffer.byteLength(body),
        });
        callback();
    });
}

function _respond(response, payload, log, callback) {
    _respondWithHeaders(response, payload, {}, log, callback);
}

function _respondWithHeaderCrrConflict(response, log, callback, code, message, mvId) {
    return _respondWithHeaders(
        response,
        { code, message },
        { 'x-scal-micro-version-id': mvId ? encode(mvId) : '' },
        log,
        callback,
        HTTP_STATUS_CONFLICT,
    );
}

function _getRequestPayload(req, cb) {
    const payload = [];
    let payloadLen = 0;
    req.on('data', chunk => {
        payload.push(chunk);
        payloadLen += chunk.length;
    })
        .on('error', cb)
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
        return errorInstances.BadRequest.customizeDescription(errMessage);
    }
    if (operation === 'putpart' && headers['x-scal-part-number'] === undefined) {
        errMessage = 'bad request: missing part-number header';
        log.error(errMessage);
        return errorInstances.BadRequest.customizeDescription(errMessage);
    }
    const isMPUOperation = ['putpart', 'completempu', 'abortmpu'].includes(operation);
    if (isMPUOperation && headers['x-scal-upload-id'] === undefined) {
        errMessage = 'bad request: missing upload-id header';
        log.error(errMessage);
        return errorInstances.BadRequest.customizeDescription(errMessage);
    }
    if (operation === 'putobject' && headers['x-scal-canonical-id'] === undefined) {
        errMessage = 'bad request: missing x-scal-canonical-id header';
        log.error(errMessage);
        return errorInstances.BadRequest.customizeDescription(errMessage);
    }
    // Ensure the external backend has versioning before asserting version ID.
    if (
        !constants.versioningNotImplBackends[storageType] &&
        (operation === 'puttagging' || operation === 'deletetagging')
    ) {
        if (headers['x-scal-data-store-version-id'] === undefined) {
            errMessage = 'bad request: missing x-scal-data-store-version-id header';
            log.error(errMessage);
            return errorInstances.BadRequest.customizeDescription(errMessage);
        }
        if (headers['x-scal-source-bucket'] === undefined) {
            errMessage = 'bad request: missing x-scal-source-bucket header';
            log.error(errMessage);
            return errorInstances.BadRequest.customizeDescription(errMessage);
        }
        if (headers['x-scal-replication-endpoint-site'] === undefined) {
            errMessage = 'bad request: missing ' + 'x-scal-replication-endpoint-site';
            log.error(errMessage);
            return errorInstances.BadRequest.customizeDescription(errMessage);
        }
    }
    if (operation === 'putobject' && headers['content-md5'] === undefined) {
        errMessage = 'bad request: missing content-md5 header';
        log.error(errMessage);
        return errorInstances.BadRequest.customizeDescription(errMessage);
    }
    if (headers['x-scal-storage-class'] === undefined) {
        errMessage = 'bad request: missing x-scal-storage-class header';
        log.error(errMessage);
        return errorInstances.BadRequest.customizeDescription(errMessage);
    }
    const location = locationConstraints[headers['x-scal-storage-class']];
    const storageTypeList = storageType.split(',');
    const isValidLocation = location && storageTypeList.includes(location.type);
    if (!isValidLocation) {
        errMessage = 'invalid request: invalid location constraint in request';
        log.debug(errMessage, {
            method: request.method,
            bucketName: request.bucketName,
            objectKey: request.objectKey,
            resourceType: request.resourceType,
        });
        return errorInstances.InvalidRequest.customizeDescription(errMessage);
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
        aggregateSize = parts.reduce((agg, part) => agg + Number.parseInt(part.Size[0], 10), 0);
    }
    return {
        aggregateSize,
        aggregateETag: s3middleware.processMpuParts.createAggregateETag(parts.map(part => part.ETag[0])),
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

    const location = [
        {
            dataStoreName: params.dataStoreName,
            dataStoreType: params.dataStoreType,
            key: params.key,
            start: 0,
            size: params.size,
            dataStoreETag: params.dataStoreETag,
            dataStoreVersionId: params.dataStoreVersionId,
        },
    ];
    return {
        // TODO: Remove '' when versioning implemented for Azure.
        versionId: params.dataStoreVersionId || '',
        location,
    };
}

function handleTaggingOperation(request, response, type, dataStoreVersionId, log, callback) {
    const storageLocation = request.headers['x-scal-storage-class'];
    const objectMD = {
        dataStoreName: storageLocation,
        location: [{ dataStoreVersionId }],
    };
    if (type === 'Put') {
        try {
            const tags = JSON.parse(request.headers['x-scal-tags']);
            objectMD.tags = tags;
        } catch {
            // FIXME: add error type MalformedJSON
            return callback(errors.MalformedPOSTRequest);
        }
    }
    return dataClient.objectTagging(type, request.objectKey, request.bucketName, objectMD, log, err => {
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
        return response ? _respond(response, dataRetrievalInfo, log, callback) : callback();
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
POST /_/backbeat/batchdelete/<bucket name>
GET /_/backbeat/lifecycle/<bucket name>?list-type=current
GET /_/backbeat/lifecycle/<bucket name>?list-type=noncurrent
GET /_/backbeat/lifecycle/<bucket name>?list-type=orphan
POST /_/backbeat/index/<bucket name>?operation=add
POST /_/backbeat/index/<bucket name>?operation=delete
GET /_/backbeat/index/<bucket name>
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
    } catch {
        const msg = 'x-scal-locations header is invalid';
        return cb(errorInstances.InvalidRequest.customizeDescription(msg));
    }
    if (!Array.isArray(locations)) {
        const msg = 'x-scal-locations header is invalid';
        return cb(errorInstances.InvalidRequest.customizeDescription(msg));
    }
    return _getLastModified(locations, log, (err, lastModified) => {
        if (err) {
            return cb(err);
        }
        const dataRetrievalInfo = { lastModified };
        return _respond(response, dataRetrievalInfo, log, cb);
    });
}

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
    if (request.serverAccessLog) {
        // eslint-disable-next-line no-param-reassign
        request.serverAccessLog.replication = true;
    }
    let errMessage;
    const canonicalID = request.headers['x-scal-canonical-id'];
    if (canonicalID === undefined) {
        errMessage = 'bad request: missing x-scal-canonical-id header';
        log.error(errMessage);
        return callback(errorInstances.BadRequest.customizeDescription(errMessage));
    }
    const contentMD5 = request.headers['content-md5'];
    if (contentMD5 === undefined) {
        errMessage = 'bad request: missing content-md5 header';
        log.error(errMessage);
        return callback(errorInstances.BadRequest.customizeDescription(errMessage));
    }

    const incomingVersionIdEncoded = request.headers['x-scal-version-id'];
    if (incomingVersionIdEncoded !== undefined) {
        const incomingVersionIdDecoded =
            incomingVersionIdEncoded !== 'null' ? decode(incomingVersionIdEncoded) : 'null';
        if (incomingVersionIdDecoded instanceof Error) {
            log.error('crr putData: failed to decode x-scal-version-id header', {
                method: 'putData',
                error: incomingVersionIdDecoded.message,
            });
            return callback(
                errorInstances.BadRequest.customizeDescription('bad request: invalid x-scal-version-id header'),
            );
        }
        if (objMd && objMd.versionId === incomingVersionIdDecoded) {
            // Data already at destination for this version; return 409 with the existing
            // microVersionId so backbeat can decide if putMetadata is still needed.
            log.debug('crr putData: version already at destination', {
                method: 'putData',
                bucketName: request.bucketName,
                objectKey: request.objectKey,
                hasMicroVersionId: !!objMd.microVersionId,
            });
            request.resume();
            return _respondWithHeaderCrrConflict(
                response,
                log,
                callback,
                VersionIdCollisionException.name,
                'version id already at destination',
                objMd.microVersionId,
            );
        }
    }

    writeContinue(request, response);
    const context = {
        bucketName: request.bucketName,
        owner: canonicalID,
        namespace: NAMESPACE,
        objectKey: request.objectKey,
    };
    const payloadLen = parseInt(request.headers['content-length'], 10);
    const backendInfoObj = locationConstraintCheck(request, null, bucketInfo, log);
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
        const headerChecksum = getChecksumDataFromHeaders(request.headers);
        if (headerChecksum && headerChecksum.error) {
            return callback(arsenalErrorFromChecksumError(headerChecksum));
        }
        const checksums = {
            primary: headerChecksum || defaultChecksumData,
            secondary: null,
        };
        return dataStore(
            context,
            cipherBundle,
            request,
            payloadLen,
            {},
            backendInfo,
            checksums,
            log,
            // The callback's 4th arg (checksum) is intentionally ignored: any
            // x-amz-checksum-* header sent by Backbeat is validated inside
            // dataStore by ChecksumTransform. The computed value is not stored
            // here because this is a data-only write — metadata is written
            // separately by Backbeat, which should propagate the source
            // object's checksum.
            (err, retrievalInfo, md5) => {
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
                const dataRetrievalInfo = [
                    {
                        key,
                        dataStoreName,
                    },
                ];
                if (cipherBundle) {
                    dataRetrievalInfo[0].cryptoScheme = cipherBundle.cryptoScheme;
                    dataRetrievalInfo[0].cipheredDataKey = cipherBundle.cipheredDataKey;
                    return _respondWithHeaders(
                        response,
                        dataRetrievalInfo,
                        {
                            'x-amz-server-side-encryption': cipherBundle.algorithm,
                            'x-amz-server-side-encryption-aws-kms-key-id': cipherBundle.masterKeyId,
                        },
                        log,
                        callback,
                    );
                }
                return _respond(response, dataRetrievalInfo, log, callback);
            },
        );
    });
}

/**
 * @callback CanonicalIdCallback
 * @param {Error|null} err - Error object if operation failed, null otherwise
 * @param {Object} [result] - Result object containing account information
 * @param {string} [result.accountId] - The account ID
 * @param {string} [result.canonicalId] - The canonical ID associated with the account
 * @param {string} [result.name] - The display name associated with the account
 * @returns {undefined}
 */

/**
 *
 * @param {string} accountId - account ID
 * @param {Log} log - logger instance
 * @param {CanonicalIdCallback} cb - callback function
 * @returns {undefined}
 */
function getCanonicalIdsByAccountId(accountId, log, cb) {
    return vault.getCanonicalIdsByAccountIds([accountId], log, (err, res) => {
        if (err) {
            log.error('error getting canonical ID by account ID', {
                error: err,
                accountId,
                method: 'getCanonicalIdsByAccountIds',
            });
            return cb(err);
        }
        if (res.length === 0) {
            log.error('account ID not found', {
                accountId,
                method: 'getCanonicalIdsByAccountIds',
            });
            return cb(errors.AccountNotFound);
        }
        return cb(null, res[0]);
    });
}

function putMetadata(request, response, bucketInfo, objMd, log, callback) {
    const { bucketName, objectKey } = request;

    const encodedMicroVersionId = request.headers['x-scal-micro-version-id'];
    // When the header is present this is a conditional write: accept only if
    // the incoming microVersionId is newer than the one already stored.
    // '' is a valid value meaning the source has no microVersionId (null revision).
    // Note: '' is falsy so the presence check must be !== undefined, not truthy.
    const hasMicroVersionId = encodedMicroVersionId !== undefined;
    let incomingMicroVersionId = null;
    if (hasMicroVersionId && objMd) {
        // '' means source has no microVersionId, treated as older revision
        incomingMicroVersionId = encodedMicroVersionId === '' ? null : decode(encodedMicroVersionId);
        if (incomingMicroVersionId instanceof Error) {
            log.error('putMetadata: failed to decode x-scal-micro-version-id header', {
                method: 'putMetadata',
                error: incomingMicroVersionId.message,
            });
            return callback(
                errorInstances.BadRequest.customizeDescription('bad request: invalid x-scal-micro-version-id header'),
            );
        }

        const isIncomingOlderThanCurrent = (incoming, current) => {
            if (current === null) {
                // nothing to be stale against
                return false;
            }
            // null incoming : source has no cascade revision => oldest possible state
            // larger string = older revision (reverse-chronological order)
            return incoming === null || incoming > current;
        };

        const objectMicroVersionId = objMd.microVersionId || null;
        let conflictErr = null;
        if (incomingMicroVersionId === objectMicroVersionId) {
            conflictErr = {
                err: MicroVersionIdAlreadyStoredException.name,
                message: 'incoming microVersionId already at destination',
            };
        } else if (isIncomingOlderThanCurrent(incomingMicroVersionId, objectMicroVersionId)) {
            conflictErr = {
                err: StaleMicroVersionIdException.name,
                message: 'incoming revision is older than destination',
                mvId: objMd?.microVersionId,
            };
        }
        if (conflictErr) {
            log.debug(`putMetadata: ${conflictErr.message}`, {
                method: 'putMetadata',
                bucketName,
                objectKey,
            });
            request.resume();
            return _respondWithHeaderCrrConflict(
                response,
                log,
                callback,
                conflictErr.err,
                conflictErr.message,
                conflictErr.mvId,
            );
        }
    }

    return _getRequestPayload(request, (err, payload) => {
        if (err) {
            return callback(err);
        }

        let omVal;

        try {
            omVal = JSON.parse(payload);
        } catch {
            return callback(errors.MalformedPOSTRequest);
        }

        if (omVal.replicationInfo?.status === 'REPLICA') {
            omVal.replicationInfo.isReplica = true;
        }

        if (incomingMicroVersionId !== null && incomingMicroVersionId !== (omVal.microVersionId ?? null)) {
            return callback(
                errors.BadRequest.customizeDescription(
                    'bad request: x-scal-micro-version-id header does not match body microVersionId',
                ),
            );
        }

        const { headers } = request;

        // Destination-side delete-marker replication.
        // We need the REPLICA status to distinguish from
        // source-side replication status updates that also carry isDeleteMarker=true.
        if (
            omVal.isDeleteMarker &&
            omVal.replicationInfo &&
            omVal.replicationInfo.isReplica &&
            request.serverAccessLog
        ) {
            // eslint-disable-next-line no-param-reassign
            request.serverAccessLog.replication = true;
            // eslint-disable-next-line no-param-reassign
            request.serverAccessLog.deleteMarker = true;
        }

        // Destination-side tag-only replication.
        // AWS uses REST.PUT.OBJECT_TAGGING for both - a tag-delete
        // is replicated as a PUT of an empty tag set with the same
        // URI shape.
        // The REPLICA status excludes source-side replication-status updates.
        if (
            omVal.replicationInfo &&
            omVal.replicationInfo.isReplica &&
            (omVal.originOp === 's3:ObjectTagging:Put' || omVal.originOp === 's3:ObjectTagging:Delete') &&
            request.serverAccessLog
        ) {
            // eslint-disable-next-line no-param-reassign
            request.serverAccessLog.replication = true;
            // eslint-disable-next-line no-param-reassign
            request.serverAccessLog.tagging = true;
        }

        // Destination-side ACL-only replication.
        // AWS uses REST.PUT.ACL on the destination with URI
        // PUT /<bucket>/<key>?acl&versionId=<srcVersionId> and
        // populates the aclRequired field.
        // The REPLICA status excludes source-side replication-status updates.
        if (
            omVal.replicationInfo &&
            omVal.replicationInfo.isReplica &&
            omVal.originOp === 's3:ObjectAcl:Put' &&
            request.serverAccessLog
        ) {
            // eslint-disable-next-line no-param-reassign
            request.serverAccessLog.replication = true;
            // eslint-disable-next-line no-param-reassign
            request.serverAccessLog.acl = true;
            // eslint-disable-next-line no-param-reassign
            request.serverAccessLog.aclRequired = 'Yes';
        }

        if (headers['x-scal-replication-content'] === 'METADATA') {
            if (!objMd) {
                return callback(errors.ObjNotFound);
            }

            [
                'location',
                'x-amz-server-side-encryption',
                'x-amz-server-side-encryption-aws-kms-key-id',
                'x-amz-server-side-encryption-customer-algorithm',
            ].forEach(headerName => {
                omVal[headerName] = objMd[headerName];
            });
        }

        let versionId = decodeVersionId(request.query);
        let versioning = bucketInfo.isVersioningEnabled();
        let isNull = false;

        if (versionId === 'null') {
            isNull = true;
            // Retrieve the null version id from the object metadata.
            versionId = objMd && objMd.versionId;
            if (!versionId) {
                // Set isNull in the object metadata to be written.
                // Since metadata will generate a versionId for the null version,
                // the flag is needed to allow cloudserver to know that the version
                // is a null version and allow access to it using the "null" versionId.
                omVal.isNull = true;
                // If the new null keys logic (S3C-7352) is supported (not compatibility mode),
                // create a null key with the isNull2 flag.
                if (!nullVersionCompatMode) {
                    omVal.isNull2 = true;
                }
                // Delete the version id from the version metadata payload to prevent issues
                // with creating a non-version object (versioning set to false) that includes a version id.
                // For example, this version ID might come from a null version of a suspended bucket being
                // replicated to this bucket.
                delete omVal.versionId;
                if (versioning) {
                    // If the null version does not have a version id, it is a current null version.
                    // To update the metadata of a current version, versioning is set to false.

                    // This condition is to handle the case where a single null version looks like a master
                    // key and will not have a duplicate versioned key and no version ID.
                    // They are created when you have a non-versioned bucket with objects,
                    // and then convert bucket to versioned.
                    // If no new versioned objects are added for given object(s), they look like
                    // standalone master keys.
                    versioning = false;
                } else {
                    const versioningConf = bucketInfo.getVersioningConfiguration();
                    // The purpose of this condition is to address situations in which
                    // - versioning is "suspended" and
                    // - no existing object or no null version.
                    // In such scenarios, we generate a new null version and designate it as the master version.
                    if (versioningConf && versioningConf.Status === 'Suspended') {
                        versionId = '';
                    }
                }
            }
        }

        // If the object is from a source bucket without versioning (i.e. NFS),
        // then we want to create a version for the replica object even though
        // none was provided in the object metadata value.
        if (omVal.replicationInfo.isNFS) {
            versioning = omVal.replicationInfo.isReplica;
            omVal.replicationInfo.isNFS = !omVal.replicationInfo.isReplica;
        }

        const options = {
            overheadField: constants.overheadField,
        };

        // NOTE: When 'versioning' is set to true and no 'versionId' is specified,
        // it results in the creation of a "new" version, which also updates the master.
        // NOTE: Since option fields are converted to strings when they're sent to Metadata via the query string,
        // Metadata interprets the value "false" as if it were true.
        // Therefore, to avoid this confusion, we don't pass the versioning parameter at all if its value is false.
        if (versioning) {
            options.versioning = true;
        }

        // NOTE: When options fields are sent to Metadata through the query string,
        // they are converted to strings. As a result, Metadata interprets the value undefined
        // in the versionId field as an empty string ('').
        // To prevent this, the versionId field is only included in options when it is defined.
        if (versionId !== undefined) {
            options.versionId = versionId;
            omVal.versionId = versionId;

            if (isNull) {
                if (!nullVersionCompatMode) {
                    omVal.isNull2 = true;
                }

                omVal.isNull = isNull;
            }

            // In the MongoDB metadata backend, setting the versionId option leads to the creation
            // or update of the version object, the master object is only updated if its versionId
            // is the same as the version. This can lead to inconsistencies when replicating objects
            // in the wrong order (can happen when retrying after failures), where the master object
            // might point to a non current version. To prevent this, we need to set repairMaster to
            // true which will update the master to the latest version available at the time of the put.
            // The update is only done when putting a new version as updating versions that already exist
            // shouldn't affect the master.
            if (!objMd) {
                options.repairMaster = true;
            }
        }

        // If the new null keys logic (S3C-7352) is not supported (compatibility mode), 'isNull' remains undefined.
        if (!nullVersionCompatMode) {
            options.isNull = isNull;
        }

        const isReplicationWrite = !!headers['x-scal-replication-content'];
        if (isReplicationWrite) {
            const isMDOnly = headers['x-scal-replication-content'] === 'METADATA';
            const objSize = omVal['content-length'] || 0;

            const nextReplInfo = getReplicationInfo(
                config,
                objectKey,
                bucketInfo,
                isMDOnly,
                objSize,
                null,
                null,
                null,
                constants.crrCascadeBlockedLocationTypes,
            );

            const hasNextHop = nextReplInfo && nextReplInfo.backends.length > 0;

            // Replicating further requires x-scal-micro-version-id for loop detection.
            if (hasNextHop && !hasMicroVersionId) {
                return callback(
                    errors.InternalError.customizeDescription(
                        'x-scal-micro-version-id is required when replication would trigger a next hop',
                    ),
                );
            }

            omVal.replicationInfo = hasNextHop
                ? nextReplInfo
                : {
                      status: '',
                      backends: [],
                      content: [],
                      destination: undefined,
                      storageClass: undefined,
                      role: undefined,
                      storageType: undefined,
                      dataStoreVersionId: undefined,
                      isNFS: undefined,
                  };
            omVal.replicationInfo.isReplica = true;
        }

        return async.series(
            [
                // Zenko's CRR delegates replacing the account
                // information to the destination's Cloudserver, as
                // Vault admin APIs are not exposed externally.
                next => {
                    // Internal users of this API (other features in Zenko) will
                    // not provide the accountId in the request, as they only update
                    // the metadata of existing objects, so there is no need to
                    // replace the account information.
                    if (!request.query?.accountId) {
                        return next();
                    }

                    return getCanonicalIdsByAccountId(request.query.accountId, log, (err, res) => {
                        if (err) {
                            return next(err);
                        }
                        omVal['owner-display-name'] = res.name;
                        omVal['owner-id'] = res.canonicalId;
                        return next();
                    });
                },
                async () => {
                    // If we create a new version of an object (so objMd is null),
                    // we should make sure that the masterVersion is versioned.
                    // If an object already exists, we just want to update the metadata
                    // of the existing object and not create a new one
                    if (versioning && !objMd) {
                        let masterMD;

                        try {
                            masterMD = await metadata.getObjectMDPromised(bucketName, objectKey, {}, log);
                        } catch (err) {
                            if (err.is?.NoSuchKey) {
                                log.debug('no master found for versioned object', {
                                    method: 'putMetadata',
                                    bucketName,
                                    objectKey,
                                });
                            } else {
                                throw err;
                            }
                        }

                        if (!masterMD) {
                            return;
                        }

                        const versioningPreprocessingResult = await versioningPreprocessingPromised(
                            bucketName,
                            bucketInfo,
                            objectKey,
                            masterMD,
                            log,
                        );

                        if (versioningPreprocessingResult) {
                            options.deleteNullKey = versioningPreprocessingResult.deleteNullKey;

                            // The master references a null version only via extraMD,
                            // which is set solely in nullVersionCompatMode. In null-key
                            // mode there is no extraMD and the master must NOT carry
                            // nullVersionId (mirrors the normal createAndStoreObject path).
                            if (versioningPreprocessingResult.extraMD) {
                                Object.assign(omVal, versioningPreprocessingResult.extraMD);
                            }
                        }
                    }
                },
                next => {
                    log.trace('putting object version', {
                        objectKey: request.objectKey,
                        omVal,
                        options,
                    });
                    return metadata.putObjectMD(bucketName, objectKey, omVal, options, log, (err, md) => {
                        if (err) {
                            // Handle duplicate key error during repair operation
                            // This can happen due to race conditions when multiple operations
                            // try to repair the master version simultaneously. Since repair
                            // is idempotent, if the master version already exists, we can
                            // treat this as success.
                            const errorMessage = err.message || err.toString() || '';
                            const isRepairDuplicateKeyError =
                                options.repairMaster &&
                                (errorMessage.includes('E11000') ||
                                    errorMessage.includes('duplicate key') ||
                                    errorMessage.includes('repair'));

                            if (isRepairDuplicateKeyError) {
                                log.warn('duplicate key error during repair - treating as success', {
                                    error: err,
                                    method: 'putMetadata',
                                    bucketName,
                                    objectKey,
                                    note: 'Repair operation is idempotent, master version already exists',
                                });
                                // Treat as success - the repair already completed
                                // Get the current metadata to return
                                return metadata.getObjectMD(bucketName, objectKey, {}, log, (getErr, currentMD) => {
                                    if (getErr) {
                                        log.warn('could not retrieve metadata after repair duplicate key error', {
                                            error: getErr,
                                            method: 'putMetadata',
                                        });
                                        // Still treat as success since repair likely completed
                                        return next(null, md || {});
                                    }
                                    return next(null, currentMD || md || {});
                                });
                            }

                            log.error('error putting object metadata', {
                                error: err,
                                method: 'putMetadata',
                            });
                            return next(err);
                        }
                        pushReplicationMetric(objMd, omVal, bucketName, objectKey, log);
                        if (
                            objMd &&
                            headers['x-scal-replication-content'] !== 'METADATA' &&
                            versionId &&
                            // The new data location is set to null when archiving to a Cold site.
                            // In that case "removing old data location key" is handled by the lifecycle
                            // transition processor. Check the content-length as a null location can
                            // also be from an empty object.
                            (omVal['content-length'] === 0 || (omVal.location && Array.isArray(omVal.location))) &&
                            locationKeysHaveChanged(objMd.location, omVal.location)
                        ) {
                            log.info('removing old data locations', {
                                method: 'putMetadata',
                                bucketName,
                                objectKey,
                            });
                            async.eachLimit(
                                objMd.location,
                                5,
                                (loc, nextEach) =>
                                    dataWrapper.data.delete(loc, log, err => {
                                        if (err) {
                                            log.warn('error removing old data location key', {
                                                bucketName,
                                                objectKey,
                                                locationKey: loc,
                                                error: err.message,
                                            });
                                        }
                                        // do not forward the error to let other
                                        // locations be deleted
                                        nextEach();
                                    }),
                                () => {
                                    log.debug('done removing old data locations', {
                                        method: 'putMetadata',
                                        bucketName,
                                        objectKey,
                                    });
                                },
                            );
                        }
                        return _respond(response, md, log, next);
                    });
                },
            ],
            callback,
        );
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
        } catch {
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
        } catch {
            // FIXME: add error type MalformedJSON
            return callback(errors.MalformedPOSTRequest);
        }
    }
    const payloadLen = parseInt(request.headers['content-length'], 10);
    const backendInfo = new BackendInfo(config, storageLocation);
    const headerChecksum = getChecksumDataFromHeaders(request.headers);
    if (headerChecksum && headerChecksum.error) {
        return callback(arsenalErrorFromChecksumError(headerChecksum));
    }
    const checksums = {
        primary: headerChecksum || defaultChecksumData,
        secondary: null,
    };
    return dataStore(
        context,
        CIPHER,
        request,
        payloadLen,
        {},
        backendInfo,
        checksums,
        log,
        // The callback's 4th arg (checksum) is intentionally ignored: any
        // x-amz-checksum-* header sent by Backbeat is validated inside
        // dataStore by ChecksumTransform. The computed value is not stored
        // here because this is a data-only write to an external backend —
        // metadata is managed separately by Backbeat, which should propagate
        // the source object's checksum.
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
                dataStoreETag: retrievalInfo.dataStoreETag ? `1:${retrievalInfo.dataStoreETag}` : `1:${md5}`,
                dataStoreVersionId: retrievalInfo.dataStoreVersionId,
            });
            return _respond(response, responsePayload, log, callback);
        },
    );
}

function deleteObjectFromExpiration(request, response, userInfo, log, callback) {
    if (request.serverAccessLog) {
        // eslint-disable-next-line no-param-reassign
        request.serverAccessLog.expiration = true;
    }
    return objectDeleteInternal(userInfo, request, log, true, err => {
        if (err) {
            log.error('error deleting object from expiration', {
                error: err,
                method: 'deleteObjectFromExpiration',
            });
            return callback(err);
        }
        return _respond(response, {}, log, callback);
    });
}

function deleteObject(request, response, log, callback) {
    const err = _checkMultipleBackendRequest(request, log);
    if (err) {
        return callback(err);
    }
    const storageLocation = request.headers['x-scal-storage-class'];
    const objectGetInfo = dataClient.toObjectGetInfo(request.objectKey, request.bucketName, storageLocation);
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
        } catch {
            // FIXME: add error type MalformedJSON
            return callback(errors.MalformedPOSTRequest);
        }
    }
    let tagging;
    if (tags !== undefined) {
        try {
            const parsedTags = JSON.parse(request.headers['x-scal-tags']);
            tagging = querystring.stringify(parsedTags);
        } catch {
            // FIXME: add error type MalformedJSON
            return callback(errors.MalformedPOSTRequest);
        }
    }
    return dataClient.createMPU(
        request.objectKey,
        metaHeaders,
        request.bucketName,
        undefined,
        storageLocation,
        contentType,
        cacheControl,
        contentDisposition,
        contentEncoding,
        tagging,
        log,
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
        },
    );
}

function abortMultipartUpload(request, response, log, callback) {
    const err = _checkMultipleBackendRequest(request, log);
    if (err) {
        return callback(err);
    }
    const storageLocation = request.headers['x-scal-storage-class'];
    const uploadId = request.headers['x-scal-upload-id'];
    return dataClient.abortMPU(request.objectKey, uploadId, storageLocation, request.bucketName, log, err => {
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
    return dataClient.uploadPart(
        undefined,
        {},
        request,
        payloadLen,
        storageLocation,
        request.objectKey,
        uploadId,
        partNumber,
        request.bucketName,
        log,
        (err, data) => {
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
        },
    );
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
        } catch {
            // FIXME: add error type MalformedJSON
            return callback(errors.MalformedPOSTRequest);
        }
        const partList = getPartList(parts, request.objectKey, uploadId, storageLocation);
        // Azure client will set user metadata at this point.
        const metaHeaders = { 'x-amz-meta-scal-replication-status': 'REPLICA' };
        if (sourceVersionId) {
            metaHeaders['x-amz-meta-scal-version-id'] = sourceVersionId;
        }
        if (userMetadata !== undefined) {
            try {
                const metaData = JSON.parse(userMetadata);
                Object.assign(metaHeaders, metaData);
            } catch {
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
            } catch {
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
        return dataClient.completeMPU(
            request.objectKey,
            uploadId,
            storageLocation,
            partList,
            undefined,
            request.bucketName,
            metaHeaders,
            contentSettings,
            tagging,
            log,
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

                const { key, dataStoreType, dataStoreVersionId } = retrievalInfo;
                let size;
                let dataStoreETag;
                if (skipMpuPartProcessing(retrievalInfo)) {
                    size = retrievalInfo.contentLength;
                    dataStoreETag = retrievalInfo.eTag;
                } else {
                    const { aggregateSize, aggregateETag } = generateMpuAggregateInfo(parts);
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
            },
        );
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
        return metadataGetObject(sourceBucket, request.objectKey, sourceVersionId, null, log, (err, objMD) => {
            if (err) {
                return callback(err);
            }
            if (!objMD) {
                return callback(errors.NoSuchKey);
            }
            const backend = objMD.replicationInfo.backends.find(o => o.site === site);
            dataStoreVersionId = backend.dataStoreVersionId;
            return handleTaggingOperation(request, response, 'Put', dataStoreVersionId, log, callback);
        });
    }
    return handleTaggingOperation(request, response, 'Put', dataStoreVersionId, log, callback);
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
        return metadataGetObject(sourceBucket, request.objectKey, sourceVersionId, null, log, (err, objMD) => {
            if (err) {
                return callback(err);
            }
            if (!objMD) {
                return callback(errors.NoSuchKey);
            }
            const backend = objMD.replicationInfo.backends.find(o => o.site === site);
            dataStoreVersionId = backend.dataStoreVersionId;
            return handleTaggingOperation(request, response, 'Delete', dataStoreVersionId, log, callback);
        });
    }
    return handleTaggingOperation(request, response, 'Delete', dataStoreVersionId, log, callback);
}

function _createAzureConditionalDeleteObjectGetInfo(request) {
    const { objectKey, bucketName, headers } = request;
    const objectGetInfo = dataClient.toObjectGetInfo(objectKey, bucketName, headers['x-scal-storage-class']);
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
            log.error('precondition for Azure deletion was not met', {
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
    return handleTaggingOperation(request, null, 'Put', undefined, log, err => {
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
            log.debug('object has been modified, skipping tagging operation', {
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
        log.debug('unknown last modified time, skipping conditional delete', {
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
    const type = storageClass && locationConstraints[storageClass] && locationConstraints[storageClass].type;
    const isExternalBackend = type && constants.externalBackends[type];
    const isNotVersioned = !locations[0].dataStoreVersionId;
    return isExternalBackend && isNotVersioned;
}

function batchDelete(request, response, userInfo, log, callback) {
    return _getRequestPayload(request, (err, payload) => {
        if (err) {
            return callback(err);
        }
        let parsedPayload;
        try {
            parsedPayload = JSON.parse(payload);
        } catch {
            // FIXME: add error type MalformedJSON
            return callback(errors.MalformedPOSTRequest);
        }
        if (!parsedPayload || !Array.isArray(parsedPayload.Locations)) {
            return callback(errors.MalformedPOSTRequest);
        }
        const locations = parsedPayload.Locations;
        if (_shouldConditionallyDelete(request, locations)) {
            return _performConditionalDelete(request, response, locations, log, callback);
        }
        log.trace('batch delete locations', { locations });
        return async.eachLimit(
            locations,
            5,
            (loc, next) => {
                const _loc = Object.assign({}, loc);
                if (_loc.dataStoreVersionId !== undefined) {
                    // required by cloud backends
                    _loc.deleteVersion = true;
                }
                dataWrapper.data.delete(_loc, log, err => {
                    if (err?.is?.ObjNotFound) {
                        log.info('batch delete: data location do not exist', {
                            method: 'batchDelete',
                            location: loc,
                        });
                        return next();
                    }
                    return next(err);
                });
            },
            err => {
                if (err) {
                    log.error('batch delete failed', {
                        method: 'batchDelete',
                        locations,
                        error: err,
                    });
                    return callback(err);
                }
                log.debug('batch delete successful', { locations });

                // Update inflight metrics for the data which has just been freed
                const bucket = request.bucketName;
                const contentLength = locations.reduce((length, loc) => length + loc.size, 0);

                // TODO: `bucket` should probably always be passed, to be confirmed in CLDSRV-643
                // For now be leniant and skip inflight updates if it is not specified, to avoid any
                // impact esp. on CRR
                if (!bucket || !config.isQuotaEnabled() || contentLength == 0) {
                    return _respond(response, null, log, callback);
                }

                return async.waterfall(
                    [
                        // eslint-disable-next-line no-unused-vars
                        next => metadata.getBucket(bucket, log, (err, bucketMD, raftSessionId) => next(err, bucketMD)),
                        (bucketMD, next) =>
                            quotaUtils.validateQuotas(
                                request,
                                bucketMD,
                                request.accountQuotas,
                                ['objectDelete'],
                                'objectDelete',
                                -contentLength,
                                false,
                                log,
                                next,
                            ),
                    ],
                    err => {
                        if (err) {
                            // Ignore error, as the data has been deleted already: only inflight count
                            // has not been updated, and will be eventually consistent anyway
                            log.warn('batch delete failed to update inflights', {
                                method: 'batchDelete',
                                locations,
                                error: err,
                            });
                        }

                        return _respond(response, null, log, callback);
                    },
                );
            },
        );
    });
}

function listLifecycle(request, response, userInfo, log, cb) {
    if (!request.query || !request.query['list-type']) {
        const errMessage = 'bad request: missing list-type query parameter';
        log.error(errMessage);
        return cb(errorInstances.BadRequest.customizeDescription(errMessage));
    }
    const listType = request.query['list-type'];

    let call;
    if (lifecycleTypeCalls[listType]) {
        call = lifecycleTypeCalls[listType];
    } else {
        const errMessage = `bad request: invalid list-type query parameter: ${listType}`;
        log.error(errMessage);
        return cb(errorInstances.BadRequest.customizeDescription(errMessage));
    }

    return call(userInfo, locationConstraints, request, log, (err, data) => {
        if (err) {
            log.error(`error during listing objects for lifecycle: ${listType}`, {
                error: err,
                method: 'listLifecycle',
            });
            return cb(err);
        }
        return _respond(response, data, log, cb);
    });
}

function putBucketIndexes(indexes, request, response, userInfo, log, callback) {
    metadata.putBucketIndexes(request.bucketName, indexes, log, err => {
        if (err) {
            log.error('error putting indexes', {
                error: err,
                method: 'putBucketindexes',
            });
            return callback(err);
        }

        return _respond(response, {}, log, callback);
    });
}

function getBucketIndexes(request, response, userInfo, log, callback) {
    metadata.getBucketIndexes(request.bucketName, log, (err, indexObj) => {
        if (err) {
            log.error('error getting indexes', {
                error: err,
                method: 'getBucketindexes',
            });
            return callback(err);
        }

        return _respond(response, { Indexes: indexObj }, log, callback);
    });
}

function deleteBucketIndexes(indexes, request, response, userInfo, log, callback) {
    metadata.deleteBucketIndexes(request.bucketName, indexes, log, err => {
        if (err) {
            log.error('error deleting indexes', {
                error: err,
                method: 'deleteBucketindexes',
            });
            return callback(err);
        }

        return _respond(response, {}, log, callback);
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
        index: {
            add: putBucketIndexes,
            delete: deleteBucketIndexes,
        },
    },
    DELETE: {
        expiration: deleteObjectFromExpiration,
        multiplebackenddata: {
            deleteobject: deleteObject,
            deleteobjecttagging: deleteObjectTagging,
            abortmpu: abortMultipartUpload,
        },
    },
    GET: {
        metadata: getMetadata,
        multiplebackendmetadata: headObject,
        lifecycle: listLifecycle,
        index: getBucketIndexes,
    },
};

const indexEntrySchema = joi.object({
    name: joi.string().required(),
    keys: joi
        .array()
        .items(
            joi.object({
                key: joi.string(),
                order: joi.number().valid(1, -1),
            }),
        )
        .required(),
});

const indexingSchema = joi.array().items(indexEntrySchema).min(1);

function routeIndexingAPIs(request, response, userInfo, log, callback) {
    const route = backbeatRoutes[request.method][request.resourceType];

    if (!['GET', 'POST'].includes(request.method)) {
        return callback(errors.MethodNotAllowed);
    }

    if (request.method === 'GET') {
        return route(request, response, userInfo, log, callback);
    }

    const op = request.query.operation;

    if (!op || typeof route[op] !== 'function') {
        log.error('Invalid operation parameter', { operation: op });
        return callback(errors.BadRequest);
    }

    return _getRequestPayload(request, (err, payload) => {
        if (err) {
            return callback(err);
        }

        let parsedIndex;

        try {
            parsedIndex = joi.attempt(JSON.parse(payload), indexingSchema, 'invalid payload');
        } catch (err) {
            log.error('Unable to parse index request body', { error: err });
            return callback(errors.BadRequest);
        }

        return route[op](parsedIndex, request, response, userInfo, log, callback);
    });
}

function routeBackbeatAPIProxy(request, response, requestContexts, log) {
    const path = request.url.replace('/_/backbeat/api/', '/_/');
    const { host, port } = config.backbeat;
    const target = `http://${host}:${port}${path}`;

    auth.server.doAuth(
        request,
        log,
        (err, userInfo, authorizationResults, streamingV4Params, infos) => {
            if (err) {
                log.debug('authentication error', {
                    error: err,
                    method: request.method,
                    bucketName: request.bucketName,
                    objectKey: request.objectKey,
                });
                return responseJSONBody(err, null, response, log);
            }
            // We don't use the authorization results for now
            // as the UI uses the external Cloudserver instance
            // as a proxy to access the Backbeat API service.

            // eslint-disable-next-line no-param-reassign
            request.accountQuotas = infos?.accountQuota;
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
                return responseJSONBody(errors.AccessDenied, null, response, log);
            }
            return backbeatProxy.web(request, response, { target }, err => {
                log.error('error proxying request to api server', { error: err.message });
                return responseJSONBody(errors.ServiceUnavailable, null, response, log);
            });
        },
        's3',
        requestContexts,
    );
}

function routeNonObjectRequest(request, response, userInfo, log, callback) {
    if (userInfo.getCanonicalID() === constants.publicId) {
        log.debug(`unauthenticated access to backbeat ${request.resourceType} routes`, {
            method: request.method,
            bucketName: request.bucketName,
            objectKey: request.objectKey,
        });
        return callback(errors.AccessDenied);
    }

    if (request.resourceType === 'index') {
        return routeIndexingAPIs(request, response, userInfo, log, callback);
    }

    const route = backbeatRoutes[request.method][request.resourceType];
    return route(request, response, userInfo, log, callback);
}

function routeBackbeat(clientIP, request, response, log) {
    // Attach the apiMethod method to the request, so it can used by monitoring in the server
    // eslint-disable-next-line no-param-reassign
    request.apiMethod = 'routeBackbeat';
    const contentLength = request.headers['x-amz-decoded-content-length'] || request.headers['content-length'];
    // eslint-disable-next-line no-param-reassign
    request.parsedContentLength = Number.parseInt(contentLength?.toString() ?? '', 10);

    log.debug('routing request');
    _normalizeBackbeatRequest(request);

    log.addDefaultFields({
        clientIP,
        url: request.url,
        method: 'routeBackbeat',
        resourceType: request.resourceType,
        bucketName: request.bucketName,
        objectKey: request.objectKey,
        bytesReceived: request.parsedContentLength || 0,
        bodyLength: parseInt(request.headers['content-length'], 10) || 0,
    });
    if (request.serverAccessLog) {
        // eslint-disable-next-line no-param-reassign
        request.serverAccessLog.bucketName = request.bucketName;
        // eslint-disable-next-line no-param-reassign
        request.serverAccessLog.objectKey = request.objectKey;
        // eslint-disable-next-line no-param-reassign
        request.serverAccessLog.backbeat = true;
        // eslint-disable-next-line no-param-reassign
        request.serverAccessLog.analyticsAction = 'BACKBEAT_INVALID';
    }

    const requestContexts = prepareRequestContexts('objectReplicate', request);

    if (request.resourceType === 'expiration' || request.resourceType === 'batchdelete') {
        // Reassign a specific apiMethod, as it is needed for quota evaluation (at least), where
        // "routeBackbeat" cannot be used as it is used for all backbeat API operations...
        // eslint-disable-next-line no-param-reassign
        request.apiMethod = 'objectDelete';

        // Request account quotas, as it will not be added for the 'objectReplicate' action which
        // is used by default for all backbeat operations
        requestContexts.forEach(context => {
            context._needQuota = true; // eslint-disable-line no-param-reassign
        });
    }

    // Ensure backbeat operations like expiration can properly use quotas
    // eslint-disable-next-line no-param-reassign
    request.finalizerHooks = [];

    // Extract all the _apiMethods and store them in an array
    const apiMethods = requestContexts ? requestContexts.map(context => context._apiMethod) : [];
    // Attach the names to the current request
    // eslint-disable-next-line no-param-reassign
    request.apiMethods = apiMethods;

    // proxy api requests to Backbeat API server
    if (request.resourceType === 'api') {
        if (!config.backbeat) {
            log.debug('unable to proxy backbeat api request', {
                backbeatConfig: config.backbeat,
            });
            return responseJSONBody(errors.MethodNotAllowed, null, response, log);
        }
        return routeBackbeatAPIProxy(request, response, requestContexts, log);
    }

    const useMultipleBackend = request.resourceType && request.resourceType.startsWith('multiplebackend');
    const invalidRequest =
        !request.resourceType ||
        (_isObjectRequest(request) && (!request.bucketName || !request.objectKey)) ||
        (!request.query.operation && request.resourceType === 'multiplebackenddata');
    const invalidRoute =
        backbeatRoutes[request.method] === undefined ||
        backbeatRoutes[request.method][request.resourceType] === undefined ||
        (backbeatRoutes[request.method][request.resourceType][request.query.operation] === undefined &&
            request.resourceType === 'multiplebackenddata');
    if (invalidRequest || invalidRoute) {
        log.debug(invalidRequest ? 'invalid request' : 'no such route');
        return responseJSONBody(errors.MethodNotAllowed, null, response, log);
    }

    const isObjectRequest = _isObjectRequest(request);

    if (request.serverAccessLog) {
        let route = backbeatRoutes[request.method][request.resourceType];
        if (useMultipleBackend && request.resourceType !== 'multiplebackendmetadata') {
            route = backbeatRoutes[request.method][request.resourceType][request.query.operation];
        }

        // eslint-disable-next-line no-param-reassign
        request.serverAccessLog.analyticsAction = route?.name ?? 'BACKBEAT_INVALID';
    }

    return async.waterfall(
        [
            next =>
                auth.server.doAuth(
                    request,
                    log,
                    (err, userInfo, authorizationResults, streamingV4Params, infos) => {
                        if (err) {
                            log.debug('authentication error', {
                                error: err,
                                bucketName: request.bucketName,
                                objectKey: request.objectKey,
                            });
                        }
                        if (request.serverAccessLog && userInfo) {
                            // eslint-disable-next-line no-param-reassign
                            request.serverAccessLog.authInfo = userInfo;
                            // eslint-disable-next-line no-param-reassign
                            request.serverAccessLog.analyticsAccountName = userInfo.getAccountDisplayName();
                            // eslint-disable-next-line no-param-reassign
                            request.serverAccessLog.analyticsUserName = userInfo.getIAMdisplayName();
                        }
                        // eslint-disable-next-line no-param-reassign
                        request.accountQuotas = infos?.accountQuota;
                        return next(err, userInfo, authorizationResults);
                    },
                    's3',
                    requestContexts,
                ),
            (userInfo, authorizationResults, next) =>
                handleAuthorizationResults(request, authorizationResults, apiMethods[0], undefined, log, err =>
                    next(err, userInfo),
                ),
            (userInfo, next) => {
                if (request.serverAccessLog) {
                    // eslint-disable-next-line no-param-reassign
                    request.serverAccessLog.startTurnAroundTime = process.hrtime.bigint();
                }
                // TODO: understand why non-object requests (batchdelete) were not authenticated
                if (!isObjectRequest) {
                    return routeNonObjectRequest(request, response, userInfo, log, next);
                }
                const decodedVidResult = decodeVersionId(request.query);
                if (decodedVidResult instanceof Error) {
                    log.trace('invalid versionId query', {
                        versionId: request.query.versionId,
                        error: decodedVidResult,
                    });
                    return next(errors.InvalidArgument);
                }
                const versionId = decodedVidResult;
                if (useMultipleBackend) {
                    if (request.resourceType === 'multiplebackendmetadata') {
                        return backbeatRoutes[request.method][request.resourceType](request, response, log, next);
                    }
                    return backbeatRoutes[request.method][request.resourceType][request.query.operation](
                        request,
                        response,
                        log,
                        next,
                    );
                }
                const mdValParams = {
                    bucketName: request.bucketName,
                    objectKey: request.objectKey,
                    authInfo: userInfo,
                    versionId,
                    requestType: request.apiMethods || 'ReplicateObject',
                    request,
                };
                return standardMetadataValidateBucketAndObj(
                    mdValParams,
                    request.actionImplicitDenies,
                    log,
                    (err, bucketInfo, objMd) => {
                        if (err) {
                            return next(err);
                        }
                        const versioningConfig = bucketInfo.getVersioningConfiguration();
                        // The following makes sure that only replication destination-related operations
                        // target buckets with versioning enabled.
                        const isVersioningRequired = request.headers['x-scal-versioning-required'] === 'true';
                        if (isVersioningRequired && (!versioningConfig || versioningConfig.Status !== 'Enabled')) {
                            log.debug('bucket versioning is not enabled');
                            return next(errors.InvalidBucketState);
                        }
                        return backbeatRoutes[request.method][request.resourceType](
                            request,
                            response,
                            bucketInfo,
                            objMd,
                            log,
                            next,
                        );
                    },
                );
            },
        ],
        err => {
            async.forEachLimit(
                // Finalizer hooks are used in a quota context and ensure consistent
                // metrics in case of API errors. No operation required if the API
                // completed successfully.
                request.finalizerHooks,
                5,
                (hook, done) => hook(err, done),
                () => {
                    if (err) {
                        log.error('error processing backbeat request', {
                            error: err,
                            method: request.method,
                            bucketName: request.bucketName,
                            objectKey: request.objectKey,
                        });
                        return responseJSONBody(err, null, response, log);
                    }
                    log.debug('backbeat route response sent successfully', {
                        method: request.method,
                        bucketName: request.bucketName,
                        objectKey: request.objectKey,
                    });
                    return responseJSONBody(null, null, response, log);
                },
            );
        },
    );
}

module.exports = {
    backbeatRoutes,
    routeBackbeat,
};
