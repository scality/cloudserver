const async = require('async');

const { errors, errorInstances, jsutil, versioning, s3middleware, s3routes } = require('arsenal');
const { validateObjectKeyLength } = s3routes.routesUtils;
const getMetaHeaders = s3middleware.userMetadata.getMetaHeaders;
const validateHeaders = s3middleware.validateConditionalHeaders;

const constants = require('../../constants');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const locationConstraintCheck = require('./apiUtils/object/locationConstraintCheck');
const { checkQueryVersionId, versioningPreprocessing, decodeVID } = require('./apiUtils/object/versioning');
const getReplicationInfo = require('./apiUtils/object/getReplicationInfo');
const { data } = require('../data/wrapper');
const services = require('../services');
const { pushMetric } = require('../utapi/utilities');
const removeAWSChunked = require('./apiUtils/object/removeAWSChunked');
const { standardMetadataValidateBucketAndObj } = require('../metadata/metadataUtils');
const validateWebsiteHeader = require('./apiUtils/object/websiteServing').validateWebsiteHeader;
const { config } = require('../Config');
const monitoring = require('../utilities/monitoringHandler');
const applyZenkoUserMD = require('./apiUtils/object/applyZenkoUserMD');
const { getObjectSSEConfiguration } = require('./apiUtils/bucket/bucketEncryption');
const { setExpirationHeaders } = require('./apiUtils/object/expirationHeaders');
const { verifyColdObjectAvailable } = require('./apiUtils/object/coldStorage');
const { setSSEHeaders } = require('./apiUtils/object/sseHeaders');
const { updateEncryption } = require('./apiUtils/bucket/updateEncryption');
const { initializeInternalLogRequestQueue, queueInternalLogRequest } = require('../utilities/serverAccessLogger');
const {
    algorithms,
    arsenalErrorFromChecksumError,
    getCopyObjectChecksumAlgorithm,
} = require('./apiUtils/integrity/validateChecksums');
const ChecksumTransform = require('../auth/streamingV4/ChecksumTransform');
const ChecksumWritable = require('../auth/streamingV4/ChecksumWritable');
const { buildSourcePartsStream } = require('./apiUtils/object/sourceChecksum');
const kms = require('../kms/wrapper');

const versionIdUtils = versioning.VersionID;
const locationHeader = constants.objectLocationConstraintHeader;
const versioningNotImplBackends = constants.versioningNotImplBackends;
const externalVersioningErrorMessage =
    'We do not currently support putting a versioned object to a location-constraint of type Azure.';

/**
 * Compute the prior data locations that are orphaned.
 *
 * @param {Array} dataToDelete - prior data locations from versioningPreprocessing
 * @param {Array} newDataGetInfo - new data locations stored in the new metadata
 * @returns {Array|null} orphaned entries to batchDelete, or null if none
 */
function _orphanedDataLocations(dataToDelete, newDataGetInfo) {
    // Identity is (dataStoreName, key): the same key under a different
    // dataStoreName points to a different backend slot, so the old slot
    // is an orphan even when the key string collides. dataStoreName is a
    // location-constraint name (never contains '|'), so the delimiter is
    // unambiguous.

    // We need to normalize to support old legacy location names.
    const normalize = loc => (typeof loc === 'string' ? { key: loc } : loc);
    const locId = loc => {
        const n = normalize(loc);
        return `${n.dataStoreName}|${n.key}`;
    };
    const newIds = new Set((newDataGetInfo || []).map(locId));
    const orphans = (dataToDelete || []).filter(loc => !newIds.has(locId(loc)));
    return orphans.length > 0 ? orphans : null;
}

/**
 * Decide whether the destination's checksum needs to be recomputed by
 * streaming the source bytes through a ChecksumTransform.
 *
 * @param {object} headers - request headers
 * @param {object} sourceObjMD - source object metadata
 * @returns {boolean}
 */
function _shouldRecomputeChecksum(headers, sourceObjMD) {
    const requestedAlgo = headers['x-amz-checksum-algorithm']?.toLowerCase();
    if (
        sourceObjMD.checksum?.checksumType === 'FULL_OBJECT' &&
        (!requestedAlgo || requestedAlgo === sourceObjMD.checksum.checksumAlgorithm)
    ) {
        return false;
    }
    return true;
}

/**
 * Pick the checksum algorithm to write on CopyObject. Defaults to CRC64NVME when the source object has no checksum.
 *
 * @param {string|null} requestedAlgo - validated request algorithm
 * @param {object} sourceObjMD - source object metadata
 * @returns {string}
 */
function _getCopyObjectChecksumAlgorithm(requestedAlgo, sourceObjMD) {
    return requestedAlgo || sourceObjMD.checksum?.checksumAlgorithm || 'crc64nvme';
}

/**
 * Recompute the destination's checksum by streaming the source bytes through
 * a checksum stream. When the bytes don't have to move (copy-to-self, same
 * location, no SSE, not versioned) the source is GET'ed solely to compute the
 * new digest via a ChecksumWritable sink; the bytes are then discarded and the
 * existing data location is reused — only cloudserver's metadata is updated.
 * Otherwise the source is piped through a ChecksumTransform into a single put.
 *
 * @param {object} log - request logger
 * @param {object} request - HTTP request
 * @param {string|null} requestedAlgo - client-requested algorithm, lowercase
 * @param {object} sourceObjMD - source object metadata
 * @param {Array} dataLocator - ordered source parts
 * @param {boolean} sourceIsDestination - source key == destination key
 * @param {boolean} isVersionedObj - destination bucket has versioning enabled
 * @param {boolean} needsEncryption - destination requires SSE
 * @param {object} storeMetadataParams - mutable carrier for destination metadata
 * @param {object} destObjMD - existing destination object metadata
 * @param {object} destBucketMD - destination bucket metadata
 * @param {object} serverSideEncryption - SSE config for the destination
 * @param {object} dataStoreContext - context passed to data.put
 * @param {object} backendInfoDest - backend info for the destination
 * @param {function} next - waterfall callback
 * @return {undefined}
 */
function _recomputeChecksumAndStore(
    log,
    request,
    requestedAlgo,
    sourceObjMD,
    dataLocator,
    sourceIsDestination,
    isVersionedObj,
    needsEncryption,
    storeMetadataParams,
    destObjMD,
    destBucketMD,
    serverSideEncryption,
    dataStoreContext,
    backendInfoDest,
    next,
) {
    const algoName = _getCopyObjectChecksumAlgorithm(requestedAlgo, sourceObjMD);
    const wrapChecksumErr = err => Object.assign(err, { checksumStream: { algorithm: algoName } });

    // Copy-to-self where the bytes don't have to move (same location, no SSE
    // change, not versioned): GET the source into a ChecksumWritable to compute
    // the new digest, discard the bytes, and reuse the existing data locations.
    // No PUT, no DELETE — only the metadata gets updated.
    if (sourceIsDestination && storeMetadataParams.locationMatch && !needsEncryption && !isVersionedObj) {
        log.debug('computing checksum on copy-to-self without rewriting data', {
            algorithm: algoName,
            size: storeMetadataParams.size,
        });
        const sourceStream = buildSourcePartsStream(dataLocator, log);
        const checksumSink = new ChecksumWritable(algoName, log);
        const finish = jsutil.once(err => {
            if (err) {
                sourceStream.destroy(err);
                checksumSink.destroy(err);
                if (request.sourceServerAccessLog) {
                    // eslint-disable-next-line no-param-reassign
                    request.sourceServerAccessLog.error = err;
                }
                return next(err, destBucketMD);
            }
            // eslint-disable-next-line no-param-reassign
            storeMetadataParams.checksum = {
                algorithm: algoName,
                value: checksumSink.digest,
                type: 'FULL_OBJECT',
            };
            return next(null, storeMetadataParams, dataLocator, destObjMD, serverSideEncryption, destBucketMD);
        });
        sourceStream.once('error', finish);
        checksumSink.once('error', err => finish(wrapChecksumErr(err)));
        checksumSink.once('finish', () => finish());
        sourceStream.pipe(checksumSink);
        return undefined;
    }

    // Stream source bytes through a ChecksumTransform and write them out as a single put.
    log.debug('recomputing checksum on CopyObject', { algorithm: algoName, size: storeMetadataParams.size });
    const sourceStream = buildSourcePartsStream(dataLocator, log);
    const checksumStream = new ChecksumTransform(algoName, undefined, false, log);
    const done = jsutil.once((err, results) => {
        if (err) {
            sourceStream.destroy(err);
            checksumStream.destroy(err);
            if (request.sourceServerAccessLog) {
                // eslint-disable-next-line no-param-reassign
                request.sourceServerAccessLog.error = err;
            }
            return next(err, destBucketMD);
        }
        return next(null, storeMetadataParams, results, destObjMD, serverSideEncryption, destBucketMD);
    });
    sourceStream.once('error', done);
    checksumStream.once('error', err => done(wrapChecksumErr(err)));
    sourceStream.pipe(checksumStream);
    const doPut = cipherBundle =>
        data.put(
            cipherBundle,
            checksumStream,
            storeMetadataParams.size,
            dataStoreContext,
            backendInfoDest,
            log,
            (err, dataRetrievalInfo, hashedStream) => {
                if (err) {
                    return done(err);
                }
                // eslint-disable-next-line no-param-reassign
                storeMetadataParams.checksum = {
                    algorithm: algoName,
                    value: checksumStream.digest,
                    type: 'FULL_OBJECT',
                };
                // Prefix the part number like createAndStoreObject does, so
                // partNumber-based reads see the copy as a single-part object.
                const dataStoreETag = dataRetrievalInfo.dataStoreETag || hashedStream?.completedHash;
                const putResult = {
                    key: dataRetrievalInfo.key,
                    size: storeMetadataParams.size,
                    start: 0,
                    dataStoreName: dataRetrievalInfo.dataStoreName,
                    dataStoreType: dataRetrievalInfo.dataStoreType,
                    dataStoreETag: dataStoreETag ? `1:${dataStoreETag}` : undefined,
                    dataStoreVersionId: dataRetrievalInfo.dataStoreVersionId,
                };
                if (cipherBundle) {
                    putResult.cryptoScheme = cipherBundle.cryptoScheme;
                    putResult.cipheredDataKey = cipherBundle.cipheredDataKey;
                }
                return done(null, [putResult]);
            },
        );
    if (serverSideEncryption?.algorithm) {
        return kms.createCipherBundle(serverSideEncryption, log, (err, cipherBundle) => {
            if (err) {
                return done(err);
            }
            return doPut(cipherBundle);
        });
    }
    return doPut(null);
}

/**
 * Preps metadata to be saved (based on copy or replace request header)
 * @param {object} request - request
 * @param {object} sourceObjMD - object md of source object
 * @param {object} headers - request headers
 * @param {boolean} sourceIsDestination - whether or not source is same as
 * destination
 * @param {AuthInfo} authInfo - authInfo from Vault
 * @param {string} objectKey - destination key name
 * @param {object} sourceBucketMD - bucket metadata of source bucket
 * @param {object} destBucketMD - bucket metadata of bucket being copied to
 * @param {string} sourceVersionId - versionId of source object for copy
 * @param {object} log - logger object
 * @return {object}
 * - (storeMetadataParams
 * - sourceLocationConstraintName {string} - location type of the source
 * - OR error
 */
function _prepMetadata(
    request,
    sourceObjMD,
    headers,
    sourceIsDestination,
    authInfo,
    objectKey,
    sourceBucketMD,
    destBucketMD,
    sourceVersionId,
    log,
) {
    let whichMetadata = headers['x-amz-metadata-directive'];
    // Default is COPY
    whichMetadata = whichMetadata === undefined ? 'COPY' : whichMetadata;
    if (whichMetadata !== 'COPY' && whichMetadata !== 'REPLACE') {
        return { error: errors.InvalidArgument };
    }
    let whichTagging = headers['x-amz-tagging-directive'];
    // Default is COPY
    whichTagging = whichTagging === undefined ? 'COPY' : whichTagging;
    if (whichTagging !== 'COPY' && whichTagging !== 'REPLACE') {
        return { error: errorInstances.InvalidArgument.customizeDescription('Unknown tagging directive') };
    }
    const overrideMetadata = {};
    if (headers['x-amz-server-side-encryption']) {
        overrideMetadata['x-amz-server-side-encryption'] = headers['x-amz-server-side-encryption'];
    }
    if (headers['x-amz-storage-class']) {
        // TODO: remove in CLDSRV-639
        overrideMetadata['x-amz-storage-class'] = headers['x-amz-storage-class'];
    }
    if (headers['x-amz-website-redirect-location']) {
        overrideMetadata['x-amz-website-redirect-location'] = headers['x-amz-website-redirect-location'];
    }
    const retentionHeaders = headers['x-amz-object-lock-mode'] && headers['x-amz-object-lock-retain-until-date'];
    const legalHoldHeader = headers['x-amz-object-lock-legal-hold'];
    if ((retentionHeaders || legalHoldHeader) && !destBucketMD.isObjectLockEnabled()) {
        return {
            error: errorInstances.InvalidRequest.customizeDescription('Bucket is missing ObjectLockConfiguration'),
        };
    }
    // Cannot copy from same source and destination if no MD
    // changed and no source version id. A checksum-algorithm header is NOT a
    // change here: AWS rejects a COPY-directive self-copy regardless of any
    // requested checksum (verified). Recomputing a checksum in place requires
    // MetadataDirective=REPLACE, which is a metadata change and so is allowed.
    if (
        sourceIsDestination &&
        whichMetadata === 'COPY' &&
        Object.keys(overrideMetadata).length === 0 &&
        !sourceVersionId
    ) {
        return {
            error: errorInstances.InvalidRequest.customizeDescription(
                'This copy' +
                    ' request is illegal because it is trying to copy an ' +
                    "object to itself without changing the object's metadata, " +
                    'storage class, website redirect location or encryption ' +
                    'attributes.',
            ),
        };
    }
    // If COPY, pull all x-amz-meta keys/values from source object
    // Otherwise, pull all x-amz-meta keys/values from request headers
    const userMetadata = whichMetadata === 'COPY' ? getMetaHeaders(sourceObjMD) : getMetaHeaders(headers);
    if (userMetadata instanceof Error) {
        log.debug('user metadata validation failed', {
            error: userMetadata,
            method: 'objectCopy',
        });
        return { error: userMetadata };
    }
    // if the request occurs within a Zenko deployment, we place a user-metadata
    // field on the object
    applyZenkoUserMD(userMetadata);
    // If metadataDirective is:
    // - 'COPY' and source object has a location constraint in its metadata
    // we use the bucket destination location constraint
    if (whichMetadata === 'COPY' && userMetadata[locationHeader] && destBucketMD.getLocationConstraint()) {
        userMetadata[locationHeader] = destBucketMD.getLocationConstraint();
    }
    const backendInfoObjSource = locationConstraintCheck(request, sourceObjMD, sourceBucketMD, log);
    if (backendInfoObjSource.err) {
        return { error: backendInfoObjSource.err };
    }
    const sourceLocationConstraintName = backendInfoObjSource.controllingLC;

    const backendInfoObjDest = locationConstraintCheck(request, userMetadata, destBucketMD, log);
    if (backendInfoObjDest.err) {
        return { error: backendInfoObjDest.err };
    }
    const destLocationConstraintName = backendInfoObjDest.controllingLC;

    // If location constraint header is not included, locations match
    const locationMatch = sourceLocationConstraintName === destLocationConstraintName;

    // If tagging directive is REPLACE but you don't specify any
    // tags in the request, the destination object will
    // not have any tags.
    // If tagging directive is COPY but the source object does not have tags,
    // the destination object will not have any tags.
    let tagging;
    let taggingCopy;
    if (whichTagging === 'COPY') {
        taggingCopy = sourceObjMD.tags || {};
    } else {
        tagging = headers['x-amz-tagging'] || '';
    }

    // If COPY, pull the necessary headers from source object
    // Otherwise, pull them from request headers
    const headersToStoreSource = whichMetadata === 'COPY' ? sourceObjMD : headers;

    const storeMetadataParams = {
        objectKey,
        log,
        headers,
        authInfo,
        metaHeaders: userMetadata,
        size: sourceObjMD['content-length'],
        contentType: headersToStoreSource['content-type'],
        contentMD5: sourceObjMD['content-md5'],
        cacheControl: headersToStoreSource['cache-control'],
        contentDisposition: headersToStoreSource['content-disposition'],
        contentEncoding: removeAWSChunked(headersToStoreSource['content-encoding']),
        dataStoreName: destLocationConstraintName,
        expires: headersToStoreSource.expires,
        overrideMetadata,
        lastModifiedDate: new Date().toJSON(),
        tagging,
        taggingCopy,
        replicationInfo: getReplicationInfo(config, objectKey, destBucketMD, false, sourceObjMD['content-length']),
        locationMatch,
        originOp: 's3:ObjectCreated:Copy',
    };

    const defaultRetentionConfig = destBucketMD.getObjectLockConfiguration();
    if (defaultRetentionConfig && !legalHoldHeader) {
        storeMetadataParams.defaultRetention = defaultRetentionConfig;
    }

    if (sourceObjMD.checksum && !_shouldRecomputeChecksum(headers, sourceObjMD)) {
        storeMetadataParams.checksum = {
            algorithm: sourceObjMD.checksum.checksumAlgorithm,
            value: sourceObjMD.checksum.checksumValue,
            type: sourceObjMD.checksum.checksumType,
        };
    }

    // In case whichMetadata === 'REPLACE' but contentType is undefined in copy
    // request headers, make sure to keep the original header instead
    if (!storeMetadataParams.contentType) {
        storeMetadataParams.contentType = sourceObjMD['content-type'];
    }

    if (authInfo.getCanonicalID() !== destBucketMD.getOwner()) {
        storeMetadataParams.bucketOwnerId = destBucketMD.getOwner();
    }

    return { storeMetadataParams, sourceLocationConstraintName, backendInfoDest: backendInfoObjDest.backendInfo };
}

/**
 * PUT Object Copy in the requested bucket.
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with
 * requester's info
 * @param {request} request - request object given by router,
 *                            includes normalized headers
 * @param {string} sourceBucket - name of source bucket for object copy
 * @param {string} sourceObject - name of source object for object copy
 * @param {string} sourceVersionId - versionId of source object for copy
 * @param {object} log - the log request
 * @param {function} callback - final callback to call with the result
 * @return {undefined}
 */
function objectCopy(authInfo, request, sourceBucket, sourceObject, sourceVersionId, log, callback) {
    log.debug('processing request', { method: 'objectCopy' });
    const destBucketName = request.bucketName;
    const destObjectKey = request.objectKey;

    const keyLengthError = validateObjectKeyLength(destObjectKey, config.objectKeyByteLimit);
    if (keyLengthError) {
        return callback(keyLengthError);
    }

    const sourceIsDestination = destBucketName === sourceBucket && destObjectKey === sourceObject;
    const valGetParams = {
        authInfo,
        bucketName: sourceBucket,
        objectKey: sourceObject,
        versionId: sourceVersionId,
        getDeleteMarker: true,
        requestType: 'objectGet',
        /**
         * Authorization will first check the target object, with an objectPut
         * action. But in this context, the source object metadata is still
         * unknown. In the context of quotas, to know the number of bytes that
         * are being written, we explicitly enable the quota evaluation logic
         * during the objectGet action instead.
         */
        checkQuota: true,
        request,
    };
    const valPutParams = {
        authInfo,
        bucketName: destBucketName,
        objectKey: destObjectKey,
        requestType: 'objectPut',
        checkQuota: false,
        request,
    };
    const dataStoreContext = {
        bucketName: destBucketName,
        owner: authInfo.getCanonicalID(),
        namespace: request.namespace,
        objectKey: destObjectKey,
    };
    const websiteRedirectHeader = request.headers['x-amz-website-redirect-location'];
    const responseHeaders = {};

    if (
        request.headers['x-amz-storage-class'] &&
        !constants.validStorageClasses.includes(request.headers['x-amz-storage-class'])
    ) {
        log.trace('invalid storage-class header');
        monitoring.promMetrics('PUT', destBucketName, errorInstances.InvalidStorageClass.code, 'copyObject');
        return callback(errors.InvalidStorageClass);
    }
    if (!validateWebsiteHeader(websiteRedirectHeader)) {
        const err = errors.InvalidRedirectLocation;
        log.debug(`invalid x-amz-website-redirect-location value ${websiteRedirectHeader}`, { error: err });
        monitoring.promMetrics('PUT', destBucketName, err.code, 'copyObject');
        return callback(err);
    }
    const { error: checksumAlgoErr, algorithm: requestedAlgo } = getCopyObjectChecksumAlgorithm(request.headers);
    if (checksumAlgoErr) {
        const err = arsenalErrorFromChecksumError(checksumAlgoErr);
        log.debug('invalid x-amz-checksum-algorithm', { error: checksumAlgoErr });
        monitoring.promMetrics('PUT', destBucketName, err.code, 'copyObject');
        return callback(err);
    }
    const queryContainsVersionId = checkQueryVersionId(request.query);
    if (queryContainsVersionId instanceof Error) {
        return callback(queryContainsVersionId);
    }
    return async.waterfall(
        [
            function checkDestAuth(next) {
                return standardMetadataValidateBucketAndObj(
                    valPutParams,
                    request.actionImplicitDenies,
                    log,
                    (err, destBucketMD, destObjMD) =>
                        updateEncryption(
                            err,
                            destBucketMD,
                            destObjMD,
                            destObjectKey,
                            log,
                            { skipObject: true },
                            (err, destBucketMD, destObjMD) => {
                                if (err) {
                                    log.debug('error validating put part of request', { error: err });
                                    return next(err, destBucketMD);
                                }
                                const flag = destBucketMD.hasDeletedFlag() || destBucketMD.hasTransientFlag();
                                if (flag) {
                                    log.trace('deleted flag or transient flag on destination bucket', { flag });
                                    return next(errors.NoSuchBucket);
                                }
                                return next(null, destBucketMD, destObjMD);
                            },
                        ),
                );
            },
            function checkSourceAuthorization(destBucketMD, destObjMD, next) {
                return standardMetadataValidateBucketAndObj(
                    {
                        ...valGetParams,
                        destObjMD,
                        serverAccessLogOptions: { copySource: true },
                    },
                    request.actionImplicitDenies,
                    log,
                    (err, sourceBucketMD, sourceObjMD) => {
                        if (err) {
                            log.debug('error validating get part of request', { error: err });
                            // eslint-disable-next-line no-param-reassign
                            request.sourceServerAccessLog && (request.sourceServerAccessLog.error = err);
                            return next(err, null, destBucketMD);
                        }
                        if (!sourceObjMD) {
                            const err = sourceVersionId ? errors.NoSuchVersion : errors.NoSuchKey;
                            log.debug('no source object', { sourceObject });
                            // eslint-disable-next-line no-param-reassign
                            request.sourceServerAccessLog && (request.sourceServerAccessLog.error = err);
                            return next(err, null, destBucketMD);
                        }
                        // check if object data is in a cold storage
                        const coldErr = verifyColdObjectAvailable(sourceObjMD);
                        if (coldErr) {
                            // eslint-disable-next-line no-param-reassign
                            request.sourceServerAccessLog && (request.sourceServerAccessLog.error = coldErr);
                            return next(coldErr, null);
                        }
                        if (sourceObjMD.isDeleteMarker) {
                            log.debug('delete marker on source object', { sourceObject });
                            let err;
                            if (sourceVersionId) {
                                err = errorInstances.InvalidRequest.customizeDescription(
                                    'The source of a copy ' +
                                        'request may not specifically refer to a delete' +
                                        'marker by version id.',
                                );
                            } else {
                                // if user specifies a key in a versioned source bucket
                                // without specifying a version, and the object has
                                // a delete marker, return NoSuchKey
                                err = errors.NoSuchKey;
                            }
                            // eslint-disable-next-line no-param-reassign
                            request.sourceServerAccessLog && (request.sourceServerAccessLog.error = err);
                            return next(err, destBucketMD);
                        }
                        const sourceSize = parseInt(sourceObjMD['content-length'], 10);
                        if (sourceSize > constants.maximumAllowedUploadSize && !config.bypassMaxPutObjectSize) {
                            log.debug('copy source object too large', { sourceSize });
                            const err = errorInstances.InvalidRequest.customizeDescription(
                                'The specified copy source is larger than the maximum ' +
                                    `allowable size for a copy source: ${constants.maximumAllowedUploadSize}`,
                            );
                            if (request.sourceServerAccessLog) {
                                // eslint-disable-next-line no-param-reassign
                                request.sourceServerAccessLog.error = err;
                            }
                            return next(err, destBucketMD);
                        }
                        const headerValResult = validateHeaders(
                            request.headers,
                            sourceObjMD['last-modified'],
                            sourceObjMD['content-md5'],
                        );
                        if (headerValResult.error) {
                            request.sourceServerAccessLog &&
                                // eslint-disable-next-line no-param-reassign
                                (request.sourceServerAccessLog.error = errors.PreconditionFailed);
                            return next(errors.PreconditionFailed, destBucketMD);
                        }
                        const {
                            storeMetadataParams,
                            error: metadataError,
                            sourceLocationConstraintName,
                            backendInfoDest,
                        } = _prepMetadata(
                            request,
                            sourceObjMD,
                            request.headers,
                            sourceIsDestination,
                            authInfo,
                            destObjectKey,
                            sourceBucketMD,
                            destBucketMD,
                            sourceVersionId,
                            log,
                        );
                        if (metadataError) {
                            // eslint-disable-next-line no-param-reassign
                            request.sourceServerAccessLog && (request.sourceServerAccessLog.error = metadataError);
                            return next(metadataError, destBucketMD);
                        }
                        if (storeMetadataParams.metaHeaders) {
                            dataStoreContext.metaHeaders = storeMetadataParams.metaHeaders;
                        }

                        storeMetadataParams.overheadField = constants.overheadField;

                        let dataLocator;
                        // If 0 byte object just set dataLocator to empty array
                        if (!sourceObjMD.location) {
                            dataLocator = [];
                        } else {
                            // To provide for backwards compatibility before
                            // md-model-version 2, need to handle cases where
                            // objMD.location is just a string
                            dataLocator = Array.isArray(sourceObjMD.location)
                                ? sourceObjMD.location
                                : [{ key: sourceObjMD.location }];
                        }

                        if (sourceObjMD['x-amz-server-side-encryption']) {
                            for (let i = 0; i < dataLocator.length; i++) {
                                dataLocator[i].masterKeyId = sourceObjMD['x-amz-server-side-encryption-aws-kms-key-id'];
                                dataLocator[i].algorithm = sourceObjMD['x-amz-server-side-encryption'];
                            }
                        }

                        // If the destination key already exists
                        if (destObjMD) {
                            // Re-use creation-time if we can
                            if (destObjMD['creation-time']) {
                                storeMetadataParams.creationTime = destObjMD['creation-time'];
                                // Otherwise fallback to last-modified
                            } else {
                                storeMetadataParams.creationTime = destObjMD['last-modified'];
                            }
                            // If this is a new key, create a new timestamp
                        } else {
                            storeMetadataParams.creationTime = new Date().toJSON();
                        }

                        return next(
                            null,
                            storeMetadataParams,
                            dataLocator,
                            sourceBucketMD,
                            destBucketMD,
                            destObjMD,
                            sourceLocationConstraintName,
                            backendInfoDest,
                            sourceObjMD,
                        );
                    },
                );
            },
            function getSSEConfiguration(
                storeMetadataParams,
                dataLocator,
                sourceBucketMD,
                destBucketMD,
                destObjMD,
                sourceLocationConstraintName,
                backendInfoDest,
                sourceObjMD,
                next,
            ) {
                getObjectSSEConfiguration(request.headers, destBucketMD, log, (err, sseConfig) =>
                    next(
                        err,
                        storeMetadataParams,
                        dataLocator,
                        sourceBucketMD,
                        destBucketMD,
                        destObjMD,
                        sourceLocationConstraintName,
                        backendInfoDest,
                        sseConfig,
                        sourceObjMD,
                    ),
                );
            },
            function goGetData(
                storeMetadataParams,
                dataLocator,
                sourceBucketMD,
                destBucketMD,
                destObjMD,
                sourceLocationConstraintName,
                backendInfoDest,
                serverSideEncryption,
                sourceObjMD,
                next,
            ) {
                const vcfg = destBucketMD.getVersioningConfiguration();
                const isVersionedObj = vcfg && vcfg.Status === 'Enabled';
                const destLocationConstraintName = storeMetadataParams.dataStoreName;
                const needsEncryption = serverSideEncryption && !!serverSideEncryption.algo;
                const shouldRecomputeChecksum = _shouldRecomputeChecksum(request.headers, sourceObjMD);
                let destLocationConstraintType;
                if (config.backends.data === 'multiple') {
                    destLocationConstraintType = config.getLocationConstraintType(destLocationConstraintName);
                }
                // skip if source and dest and location constraint the same and
                // versioning is not enabled, unless we need to recompute a
                // checksum (which requires streaming the bytes through us).
                if (
                    sourceIsDestination &&
                    storeMetadataParams.locationMatch &&
                    !isVersionedObj &&
                    !needsEncryption &&
                    !shouldRecomputeChecksum
                ) {
                    return next(null, storeMetadataParams, dataLocator, destObjMD, serverSideEncryption, destBucketMD);
                }

                // also skip if 0 byte object, unless location constraint is an
                // external backend and differs from source, in which case put
                // metadata to backend
                if (
                    destLocationConstraintType &&
                    versioningNotImplBackends[destLocationConstraintType] &&
                    isVersionedObj
                ) {
                    log.debug(externalVersioningErrorMessage, {
                        method: 'multipleBackendGateway',
                        error: errors.NotImplemented,
                    });
                    return next(
                        errorInstances.NotImplemented.customizeDescription(externalVersioningErrorMessage),
                        destBucketMD,
                    );
                }
                if (dataLocator.length === 0) {
                    const finishZeroByte = () => {
                        if (
                            !storeMetadataParams.locationMatch &&
                            destLocationConstraintType &&
                            constants.externalBackends[destLocationConstraintType]
                        ) {
                            return data.put(
                                null,
                                null,
                                storeMetadataParams.size,
                                dataStoreContext,
                                backendInfoDest,
                                log,
                                (error, objectRetrievalInfo) => {
                                    if (error) {
                                        return next(error, destBucketMD);
                                    }
                                    const putResult = {
                                        key: objectRetrievalInfo.key,
                                        dataStoreName: objectRetrievalInfo.dataStoreName,
                                        dataStoreType: objectRetrievalInfo.dataStoreType,
                                        size: storeMetadataParams.size,
                                    };
                                    return next(
                                        null,
                                        storeMetadataParams,
                                        [putResult],
                                        destObjMD,
                                        serverSideEncryption,
                                        destBucketMD,
                                    );
                                },
                            );
                        }
                        return next(
                            null,
                            storeMetadataParams,
                            dataLocator,
                            destObjMD,
                            serverSideEncryption,
                            destBucketMD,
                        );
                    };
                    if (shouldRecomputeChecksum) {
                        // No bytes to stream, but AWS still writes the empty-bytes digest of the chosen algorithm.
                        const algoName = _getCopyObjectChecksumAlgorithm(requestedAlgo, sourceObjMD);
                        return Promise.resolve(algorithms[algoName].digest(Buffer.alloc(0))).then(
                            digest => {
                                // eslint-disable-next-line no-param-reassign
                                storeMetadataParams.checksum = {
                                    algorithm: algoName,
                                    value: digest,
                                    type: 'FULL_OBJECT',
                                };
                                return finishZeroByte();
                            },
                            err => {
                                log.error('failed to compute empty checksum digest', {
                                    algorithm: algoName,
                                    error: err,
                                });
                                return next(errors.InternalError, destBucketMD);
                            },
                        );
                    }
                    return finishZeroByte();
                }
                if (shouldRecomputeChecksum) {
                    return _recomputeChecksumAndStore(
                        log,
                        request,
                        requestedAlgo,
                        sourceObjMD,
                        dataLocator,
                        sourceIsDestination,
                        isVersionedObj,
                        needsEncryption,
                        storeMetadataParams,
                        destObjMD,
                        destBucketMD,
                        serverSideEncryption,
                        dataStoreContext,
                        backendInfoDest,
                        next,
                    );
                }
                const originalIdentityImpDenies = request.actionImplicitDenies;
                // eslint-disable-next-line no-param-reassign
                delete request.actionImplicitDenies;
                return data.copyObject(
                    request,
                    sourceLocationConstraintName,
                    storeMetadataParams,
                    dataLocator,
                    dataStoreContext,
                    backendInfoDest,
                    sourceBucketMD,
                    destBucketMD,
                    serverSideEncryption,
                    log,
                    (err, results) => {
                        // eslint-disable-next-line no-param-reassign
                        request.actionImplicitDenies = originalIdentityImpDenies;
                        if (err) {
                            // eslint-disable-next-line no-param-reassign
                            request.sourceServerAccessLog && (request.sourceServerAccessLog.error = err);
                            return next(err, destBucketMD);
                        }
                        return next(null, storeMetadataParams, results, destObjMD, serverSideEncryption, destBucketMD);
                    },
                );
            },
            function getVersioningInfo(
                storeMetadataParams,
                destDataGetInfoArr,
                destObjMD,
                serverSideEncryption,
                destBucketMD,
                next,
            ) {
                if (!destBucketMD.isVersioningEnabled() && destObjMD?.archive?.archiveInfo) {
                    // Ensure we trigger a "delete" event in the oplog for the previously archived object
                    // eslint-disable-next-line
                    storeMetadataParams.needOplogUpdate = 's3:ReplaceArchivedObject';
                }
                return versioningPreprocessing(
                    destBucketName,
                    destBucketMD,
                    destObjectKey,
                    destObjMD,
                    log,
                    (err, options) => {
                        if (err) {
                            log.debug('error processing versioning info', { error: err });
                            return next(err, null, destBucketMD);
                        }

                        const location = destDataGetInfoArr?.[0]?.dataStoreName;
                        if (location === destBucketMD.getLocationConstraint() && destBucketMD.isIngestionBucket()) {
                            // If the object is being written to the "ingested" storage location, keep the same
                            // versionId for consistency and to avoid creating an extra version when it gets
                            // ingested
                            const backendVersionId = decodeVID(destDataGetInfoArr[0].dataStoreVersionId);
                            if (!(backendVersionId instanceof Error)) {
                                options.versionId = backendVersionId; // eslint-disable-line no-param-reassign
                            }
                        }

                        // eslint-disable-next-line
                        storeMetadataParams.versionId = options.versionId;
                        // eslint-disable-next-line
                        storeMetadataParams.versioning = options.versioning;
                        // eslint-disable-next-line
                        storeMetadataParams.isNull = options.isNull;
                        if (options.extraMD) {
                            Object.assign(storeMetadataParams, options.extraMD);
                        }
                        const dataToDelete = _orphanedDataLocations(options.dataToDelete, destDataGetInfoArr);
                        return next(
                            null,
                            storeMetadataParams,
                            destDataGetInfoArr,
                            destObjMD,
                            serverSideEncryption,
                            destBucketMD,
                            dataToDelete,
                        );
                    },
                );
            },
            function storeNewMetadata(
                storeMetadataParams,
                destDataGetInfoArr,
                destObjMD,
                serverSideEncryption,
                destBucketMD,
                dataToDelete,
                next,
            ) {
                if (destObjMD && destObjMD.uploadId) {
                    // eslint-disable-next-line
                    storeMetadataParams.oldReplayId = destObjMD.uploadId;
                }

                return services.metadataStoreObject(
                    destBucketName,
                    destDataGetInfoArr,
                    serverSideEncryption,
                    storeMetadataParams,
                    (err, result) => {
                        if (err) {
                            log.debug('error storing new metadata', { error: err });
                            return next(err, null, destBucketMD);
                        }
                        const sourceObjSize = storeMetadataParams.size;
                        const destObjPrevSize =
                            destObjMD && destObjMD['content-length'] !== undefined ? destObjMD['content-length'] : null;

                        setExpirationHeaders(responseHeaders, {
                            lifecycleConfig: destBucketMD.getLifecycleConfiguration(),
                            objectParams: {
                                key: destObjectKey,
                                date: result.lastModified,
                                tags: result.tags,
                            },
                        });

                        return next(
                            null,
                            dataToDelete,
                            result,
                            destBucketMD,
                            storeMetadataParams,
                            serverSideEncryption,
                            sourceObjSize,
                            destObjPrevSize,
                        );
                    },
                );
            },
            function deleteExistingData(
                dataToDelete,
                storingNewMdResult,
                destBucketMD,
                storeMetadataParams,
                serverSideEncryption,
                sourceObjSize,
                destObjPrevSize,
                next,
            ) {
                if (dataToDelete) {
                    const newDataStoreName = storeMetadataParams.dataStoreName;
                    return data.batchDelete(dataToDelete, request.method, newDataStoreName, log, err => {
                        if (err) {
                            // if error, log the error and move on as it is not
                            // relevant to the client as the client's
                            // object already succeeded putting data, metadata
                            log.error('error deleting existing data', { error: err });
                        }
                        next(
                            null,
                            storingNewMdResult,
                            destBucketMD,
                            storeMetadataParams,
                            serverSideEncryption,
                            sourceObjSize,
                            destObjPrevSize,
                        );
                    });
                }
                return next(
                    null,
                    storingNewMdResult,
                    destBucketMD,
                    storeMetadataParams,
                    serverSideEncryption,
                    sourceObjSize,
                    destObjPrevSize,
                );
            },
        ],
        (
            err,
            storingNewMdResult,
            destBucketMD,
            storeMetadataParams,
            serverSideEncryption,
            sourceObjSize,
            destObjPrevSize,
        ) => {
            const corsHeaders = collectCorsHeaders(request.headers.origin, request.method, destBucketMD);

            // Store full object size for server access logs
            if (request.serverAccessLog) {
                // eslint-disable-next-line no-param-reassign
                request.serverAccessLog.objectSize = sourceObjSize;
            }

            // Initialize the queue for internal log request logging
            initializeInternalLogRequestQueue(request);
            // Queue the source-side access log (REST.COPY.OBJECT_GET)
            queueInternalLogRequest(request, {
                operation: 'REST.COPY.OBJECT_GET',
                sourceBucket,
                sourceObject,
                objectSize: sourceObjSize || null,
            });

            if (err) {
                monitoring.promMetrics('PUT', destBucketName, err.code, 'copyObject');
                return callback(err, null, corsHeaders);
            }
            let checksumXml = '';
            const checksum = storeMetadataParams?.checksum;
            const checksumAlgo = checksum && algorithms[checksum.algorithm];
            if (checksum && checksumAlgo) {
                checksumXml =
                    `<${checksumAlgo.xmlTag}>${checksum.value}</${checksumAlgo.xmlTag}>` +
                    `<ChecksumType>${checksum.type}</ChecksumType>`;
            } else if (checksum) {
                log.error('unknown checksum algorithm in source object metadata', { algorithm: checksum.algorithm });
            }
            const xml = [
                '<?xml version="1.0" encoding="UTF-8"?>',
                '<CopyObjectResult>',
                '<LastModified>',
                new Date(storeMetadataParams.lastModifiedDate).toISOString(),
                '</LastModified>',
                '<ETag>&quot;',
                storeMetadataParams.contentMD5,
                '&quot;</ETag>',
                checksumXml,
                '</CopyObjectResult>',
            ].join('');
            const additionalHeaders = corsHeaders || {};
            if (serverSideEncryption) {
                setSSEHeaders(
                    additionalHeaders,
                    serverSideEncryption.algorithm,
                    serverSideEncryption.configuredMasterKeyId || serverSideEncryption.masterKeyId,
                );
            }
            if (sourceVersionId) {
                additionalHeaders['x-amz-copy-source-version-id'] = versionIdUtils.encode(sourceVersionId);
            }
            const isVersioned = storingNewMdResult && storingNewMdResult.versionId;
            if (isVersioned) {
                additionalHeaders['x-amz-version-id'] = versionIdUtils.encode(storingNewMdResult.versionId);
            }

            Object.assign(responseHeaders, additionalHeaders);

            // Only pre-existing non-versioned objects get 0 all others use 1
            const numberOfObjects = !isVersioned && destObjPrevSize !== null ? 0 : 1;

            pushMetric('copyObject', log, {
                authInfo,
                canonicalID: destBucketMD.getOwner(),
                bucket: destBucketName,
                keys: [destObjectKey],
                newByteLength: sourceObjSize,
                oldByteLength: isVersioned ? null : destObjPrevSize,
                location: storeMetadataParams.dataStoreName,
                versionId: isVersioned ? storingNewMdResult.versionId : undefined,
                numberOfObjects,
            });
            monitoring.promMetrics(
                'PUT',
                destBucketName,
                '200',
                'copyObject',
                sourceObjSize,
                destObjPrevSize,
                isVersioned,
            );
            // Add expiration header if lifecycle enabled
            return callback(null, xml, responseHeaders);
        },
    );
}

module.exports = objectCopy;
// Exposed for unit testing only.
module.exports._orphanedDataLocations = _orphanedDataLocations;
