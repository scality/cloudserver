const assert = require('assert');
const async = require('async');
const { errors } = require('arsenal');

const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const { BackendInfo } = require('./apiUtils/object/BackendInfo');
const constants = require('../../constants');
const data = require('../data/wrapper');
const { dataStore } = require('./apiUtils/object/storeObject');
const { isBucketAuthorized } =
    require('./apiUtils/authorization/permissionChecks');
const kms = require('../kms/wrapper');
const metadata = require('../metadata/wrapper');
const { pushMetric } = require('../utapi/utilities');
const logger = require('../utilities/logger');
const { config } = require('../Config');
const multipleBackendGateway = require('../data/multipleBackendGateway');
const locationConstraintCheck
    = require('./apiUtils/object/locationConstraintCheck');

const skipError = new Error('skip');

// We pad the partNumbers so that the parts will be sorted in numerical order.
function _getPaddedPartNumber(number) {
    return `000000${number}`.substr(-5);
}

function _getOverviewKey(splitter, objectKey, uploadId) {
    return `overview${splitter}${objectKey}${splitter}${uploadId}`;
}

function _getPartKey(uploadId, splitter, paddedPartNumber) {
    return `${uploadId}${splitter}${paddedPartNumber}`;
}

/**
 * PUT part of object during a multipart upload. Steps include:
 * validating metadata for authorization, bucket existence
 * and multipart upload initiation existence,
 * store object data in datastore upon successful authorization,
 * store object location returned by datastore in metadata and
 * return the result in final cb
 *
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - request object
 * @param {object | undefined } streamingV4Params - if v4 auth,
 * object containing accessKey, signatureFromRequest, region, scopeDate,
 * timestamp, and credentialScope
 * (to be used for streaming v4 auth if applicable)
 * @param {object} log - Werelogs logger
 * @param {function} cb - final callback to call with the result
 * @return {undefined}
 */
function objectPutPart(authInfo, request, streamingV4Params, log,
    cb) {
    log.debug('processing request', { method: 'objectPutPart' });
    const size = request.parsedContentLength;

    if (Number.parseInt(size, 10) > constants.maximumAllowedPartSize) {
        log.debug('put part size too large', { size });
        return cb(errors.EntityTooLarge);
    }

    // Note: Part sizes cannot be less than 5MB in size except for the last.
    // However, we do not check this value here because we cannot know which
    // part will be the last until a complete MPU request is made. Thus, we let
    // the completeMultipartUpload API check that all parts except the last are
    // at least 5MB.

    const partNumber = Number.parseInt(request.query.partNumber, 10);
    // AWS caps partNumbers at 10,000
    if (partNumber > 10000) {
        return cb(errors.TooManyParts);
    }
    if (!Number.isInteger(partNumber) || partNumber < 1) {
        return cb(errors.InvalidArgument);
    }
    const bucketName = request.bucketName;
    assert.strictEqual(typeof bucketName, 'string');
    const canonicalID = authInfo.getCanonicalID();
    assert.strictEqual(typeof canonicalID, 'string');
    log.trace('owner canonicalid to send to data', {
        canonicalID: authInfo.getCanonicalID,
    });
    // Note that keys in the query object retain their case, so
    // `request.query.uploadId` must be called with that exact capitalization.
    const uploadId = request.query.uploadId;
    const mpuBucketName = `${constants.mpuBucketPrefix}${bucketName}`;
    const objectKey = request.objectKey;

    return async.waterfall([
        // Get the destination bucket.
        next => metadata.getBucket(bucketName, log,
            (err, destinationBucket) => {
                if (err && err.NoSuchBucket) {
                    return next(errors.NoSuchBucket, destinationBucket);
                }
                if (err) {
                    log.error('error getting the destination bucket', {
                        error: err,
                        method: 'objectPutPart::metadata.getBucket',
                    });
                    return next(err, destinationBucket);
                }
                return next(null, destinationBucket);
            }),
        // Check the bucket authorization.
        (destinationBucket, next) => {
            // For validating the request at the destinationBucket level the
            // `requestType` is the general 'objectPut'.
            const requestType = 'objectPut';
            if (!isBucketAuthorized(destinationBucket, requestType,
                canonicalID, authInfo, log)) {
                log.debug('access denied for user on bucket', { requestType });
                return next(errors.AccessDenied, destinationBucket);
            }
            return next(null, destinationBucket);
        },
        // Get bucket server-side encryption, if it exists.
        (destinationBucket, next) => {
            const encryption = destinationBucket.getServerSideEncryption();
            // If bucket has server-side encryption, pass the `res` value
            if (encryption) {
                return kms.createCipherBundle(encryption, log, (err, res) => {
                    if (err) {
                        log.error('error processing the cipher bundle for ' +
                            'the destination bucket', {
                                error: err,
                            });
                        return next(err, destinationBucket);
                    }
                    return next(null, destinationBucket, res);
                });
            }
            // The bucket does not have server-side encryption, so pass `null`
            return next(null, destinationBucket, null);
        },
        // Get the MPU shadow bucket.
        (destinationBucket, cipherBundle, next) =>
            metadata.getBucket(mpuBucketName, log,
            (err, mpuBucket) => {
                if (err && err.NoSuchBucket) {
                    return next(errors.NoSuchUpload, destinationBucket);
                }
                if (err) {
                    log.error('error getting the shadow mpu bucket', {
                        error: err,
                        method: 'objectPutPart::metadata.getBucket',
                    });
                    return next(err, destinationBucket);
                }
                let splitter = constants.splitter;
                // BACKWARD: Remove to remove the old splitter
                if (mpuBucket.getMdBucketModelVersion() < 2) {
                    splitter = constants.oldSplitter;
                }
                return next(null, destinationBucket, cipherBundle, splitter);
            }),
        // Check authorization of the MPU shadow bucket.
        (destinationBucket, cipherBundle, splitter, next) => {
            const mpuOverviewKey = _getOverviewKey(splitter, objectKey,
                uploadId);
            return metadata.getObjectMD(mpuBucketName, mpuOverviewKey, {}, log,
                (err, res) => {
                    if (err) {
                        log.error('error getting the object from mpu bucket', {
                            error: err,
                            method: 'objectPutPart::metadata.getObjectMD',
                        });
                        return next(err, destinationBucket);
                    }
                    const initiatorID = res.initiator.ID;
                    const requesterID = authInfo.isRequesterAnIAMUser() ?
                        authInfo.getArn() : authInfo.getCanonicalID();
                    if (initiatorID !== requesterID) {
                        return next(errors.AccessDenied, destinationBucket);
                    }

                    const objectLocationConstraint =
                        res.controllingLocationConstraint;
                    return next(null, destinationBucket,
                        objectLocationConstraint,
                        cipherBundle, splitter);
                });
        },
        // If data backend is backend that handles mpu (like real AWS),
        // no need to store part info in metadata
        (destinationBucket, objectLocationConstraint, cipherBundle,
        splitter, next) => {
            if (config.backends.data === 'multiple') {
                // if mpu was initiated in legacy version
                if (objectLocationConstraint === undefined) {
                    const backendInfoObj = locationConstraintCheck(request,
                        null, destinationBucket, log);
                    if (backendInfoObj.err) {
                        return process.nextTick(() => {
                            next(backendInfoObj.err);
                        });
                    }
                    // eslint-disable-next-line no-param-reassign
                    objectLocationConstraint = backendInfoObj.controllingLC;
                }
                return multipleBackendGateway.uploadPart(request,
                streamingV4Params, null, size, objectLocationConstraint,
                objectKey, uploadId, partNumber, bucketName, log,
                (err, partInfo) => {
                    if (err) {
                        log.error('error putting part to data backend', {
                            error: err,
                            method:
                            'objectPutPart::multipleBackendGateway.uploadPart',
                        });
                        return next(err, destinationBucket);
                    } else if (partInfo &&
                        partInfo.dataStoreType === 'aws_s3') {
                        // if data backend handles MPU, skip to end of waterfall
                        return next(skipError, destinationBucket,
                        partInfo.dataStoreETag);
                    } else if (partInfo && partInfo.dataStoreType === 'azure') {
                        return next(null, destinationBucket,
                            objectLocationConstraint, cipherBundle, splitter,
                            partInfo);
                    }
                    // if data backend doesn't handle MPU, continue waterfall
                    return next(null, destinationBucket,
                    objectLocationConstraint, cipherBundle, splitter, null);
                });
            }
            return next(null, destinationBucket, objectLocationConstraint,
            cipherBundle, splitter, null);
        },
        // Get any pre-existing part.
        (destinationBucket, objectLocationConstraint, cipherBundle,
            splitter, partInfo, next) => {
            const paddedPartNumber = _getPaddedPartNumber(partNumber);
            const partKey = _getPartKey(uploadId, splitter, paddedPartNumber);
            return metadata.getObjectMD(mpuBucketName, partKey, {}, log,
                (err, res) => {
                    // If there is no object with the same key, continue.
                    if (err && !err.NoSuchKey) {
                        log.error('error getting current part (if any)', {
                            error: err,
                            method: 'objectPutPart::metadata.getObjectMD',
                        });
                        return next(err, destinationBucket);
                    }
                    let prevObjectSize = null;
                    let oldLocations = null;
                    // If there is a pre-existing part, update utapi metrics and
                    // get the locations of data to be overwritten.
                    if (res) {
                        prevObjectSize = res['content-length'];
                        // Pull locations to clean up any potential orphans in
                        // data if object put is an overwrite of a pre-existing
                        // object with the same key and part number.
                        oldLocations = Array.isArray(res.partLocations) ?
                            res.partLocations : [res.partLocations];
                    }
                    return next(null, destinationBucket,
                        objectLocationConstraint, cipherBundle,
                        partKey, prevObjectSize, oldLocations, partInfo);
                });
        },
        // Store in data backend.
        (destinationBucket, objectLocationConstraint, cipherBundle,
        partKey, prevObjectSize, oldLocations, partInfo, next) => {
            // NOTE: set oldLocations to null so we do not batchDelete for now
            if (partInfo && partInfo.dataStoreType === 'azure') {
                // skip to storing metadata
                return next(null, destinationBucket, partInfo,
                  partInfo.dataStoreETag,
                  cipherBundle, partKey, prevObjectSize, null,
                  objectLocationConstraint);
            }
            const objectContext = {
                bucketName,
                owner: canonicalID,
                namespace: request.namespace,
                objectKey,
                partNumber: _getPaddedPartNumber(partNumber),
                uploadId,
            };
            const backendInfo = new BackendInfo(objectLocationConstraint);

            return dataStore(objectContext, cipherBundle, request,
                size, streamingV4Params, backendInfo, log,
                (err, dataGetInfo, hexDigest) => {
                    if (err) {
                        return next(err, destinationBucket);
                    }
                    return next(null, destinationBucket, dataGetInfo, hexDigest,
                        cipherBundle, partKey, prevObjectSize, oldLocations,
                        objectLocationConstraint);
                });
        },
        // Store data locations in metadata and delete any overwritten data.
        (destinationBucket, dataGetInfo, hexDigest, cipherBundle, partKey,
            prevObjectSize, oldLocations, objectLocationConstraint, next) => {
            // Use an array to be consistent with objectPutCopyPart where there
            // could be multiple locations.
            const partLocations = [dataGetInfo];
            if (cipherBundle) {
                const { algorithm, masterKeyId, cryptoScheme,
                    cipheredDataKey } = cipherBundle;
                partLocations[0].sseAlgorithm = algorithm;
                partLocations[0].sseMasterKeyId = masterKeyId;
                partLocations[0].sseCryptoScheme = cryptoScheme;
                partLocations[0].sseCipheredDataKey = cipheredDataKey;
            }
            const omVal = {
                // back to Version 3 since number-subparts is not needed
                'md-model-version': 3,
                partLocations,
                'key': partKey,
                'last-modified': new Date().toJSON(),
                'content-md5': hexDigest,
                'content-length': size,
            };
            return metadata.putObjectMD(mpuBucketName, partKey, omVal, {}, log,
                err => {
                    if (err) {
                        log.error('error putting object in mpu bucket', {
                            error: err,
                            method: 'objectPutPart::metadata.putObjectMD',
                        });
                        return next(err, destinationBucket);
                    }
                    return next(null, oldLocations, objectLocationConstraint,
                        destinationBucket, hexDigest, prevObjectSize);
                });
        },
        // Clean up any old data now that new metadata (with new
        // data locations) has been stored.
        (oldLocations, objectLocationConstraint, destinationBucket, hexDigest,
            prevObjectSize, next) => {
            if (oldLocations) {
                log.trace('overwriting mpu part, deleting data');
                const delLog = logger.newRequestLoggerFromSerializedUids(
                    log.getSerializedUids());
                return data.batchDelete(oldLocations, request.method,
                    objectLocationConstraint, delLog, err => {
                        if (err) {
                            // if error, log the error and move on as it is not
                            // relevant to the client as the client's
                            // object already succeeded putting data, metadata
                            log.error('error deleting existing data',
                                { error: err });
                        }
                        return next(null, destinationBucket, hexDigest,
                            prevObjectSize);
                    });
            }
            return next(null, destinationBucket, hexDigest,
                prevObjectSize);
        },
    ], (err, destinationBucket, hexDigest, prevObjectSize) => {
        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, destinationBucket);
        if (err) {
            if (err === skipError) {
                return cb(null, hexDigest, corsHeaders);
            }
            log.error('error in object put part (upload part)', {
                error: err,
                method: 'objectPutPart',
            });
            return cb(err, null, corsHeaders);
        }
        pushMetric('uploadPart', log, {
            authInfo,
            canonicalID: destinationBucket.getOwner(),
            bucket: bucketName,
            keys: [objectKey],
            newByteLength: size,
            oldByteLength: prevObjectSize,
            location: destinationBucket.getLocationConstraint(),
            numberOfObjects: 1,
        });
        return cb(null, hexDigest, corsHeaders);
    });
}

module.exports = objectPutPart;
