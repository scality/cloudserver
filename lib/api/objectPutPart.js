import assert from 'assert';
import async from 'async';
import { errors } from 'arsenal';

import constants from '../../constants';
import kms from '../kms/wrapper';
import { dataStore } from './apiUtils/object/storeObject';
import metadata from '../metadata/wrapper';
import { isBucketAuthorized } from './apiUtils/authorization/aclChecks';
import { pushMetric } from '../utapi/utilities';


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
export default function objectPutPart(authInfo, request, streamingV4Params, log,
    cb) {
    log.debug('processing request', { method: 'objectPutPart' });
    const size = request.parsedContentLength;

    // Part sizes cannot be greater than 5GB in size.
    if (Number.parseInt(size, 10) > 5368709120) {
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

    // If bucket has no server-side encryption, `cipherBundle` remains `null`.
    let cipherBundle = null;
    let splitter = constants.splitter;
    return async.waterfall([
        // Get the destination bucket.
        next => metadata.getBucket(bucketName, log, (err, bucket) => {
            if (err && err.NoSuchBucket) {
                return next(errors.NoSuchBucket);
            }
            if (err) {
                log.error('error getting the destination bucket', {
                    error: err,
                    method: 'objectPutPart::metadata.getBucket',
                });
                return next(err);
            }
            return next(null, bucket);
        }),
        // Check the bucket authorization.
        (bucket, next) => {
            // For validating the request at the destinationBucket level the
            // `requestType` is the general 'objectPut'.
            const requestType = 'objectPut';
            if (!isBucketAuthorized(bucket, requestType, canonicalID)) {
                log.debug('access denied for user on bucket', { requestType });
                return next(errors.AccessDenied);
            }
            return next(null, bucket);
        },
        // Get bucket server-side encryption, if it exists.
        (bucket, next) => {
            const encryption = bucket.getServerSideEncryption();
            if (encryption) {
                return kms.createCipherBundle(encryption, log, (err, res) => {
                    if (err) {
                        log.error('error processing the cipher bundle for ' +
                            'the destination bucket', {
                                error: err,
                            });
                    }
                    cipherBundle = res;
                    return next(err);
                });
            }
            return next();
        },
        // Get the MPU shadow bucket.
        next => metadata.getBucket(mpuBucketName, log, (err, mpuBucket) => {
            if (err && err.NoSuchBucket) {
                return next(errors.NoSuchUpload);
            }
            if (err) {
                log.error('error getting the shadow mpu bucket', {
                    error: err,
                    method: 'objectPutPart::metadata.getBucket',
                });
                return next(err);
            }
            // BACKWARD: Remove to remove the old splitter
            if (mpuBucket.getMdBucketModelVersion() < 2) {
                splitter = constants.oldSplitter;
            }
            return next();
        }),
        // Check authorization of the MPU shadow bucket.
        next => {
            const mpuOverviewKey = _getOverviewKey(splitter, objectKey,
                uploadId);
            return metadata.getObjectMD(mpuBucketName, mpuOverviewKey, log,
                (err, res) => {
                    if (err) {
                        log.error('error getting the object from mpu bucket', {
                            error: err,
                            method: 'objectPutPart::metadata.getObjectMD',
                        });
                        return next(err);
                    }
                    const initiatorID = res.initiator.ID;
                    const requesterID = authInfo.isRequesterAnIAMUser() ?
                        authInfo.getArn() : authInfo.getCanonicalID();
                    if (initiatorID !== requesterID) {
                        return next(errors.AccessDenied);
                    }
                    return next();
                });
        },
        // Store in data backend.
        next => {
            const objectKeyContext = {
                bucketName,
                owner: canonicalID,
                namespace: request.namespace,
            };
            return dataStore(objectKeyContext, cipherBundle, request, size,
                streamingV4Params, log, next);
        },
        // Store data locations in metadata.
        (dataGetInfo, hexDigest, next) => {
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
            const paddedPartNumber = _getPaddedPartNumber(partNumber);
            const partKey = _getPartKey(uploadId, splitter, paddedPartNumber);
            const omVal = {
                // Version 3 changes the format of partLocations from an object
                // to an array
                'md-model-version': 3,
                partLocations,
                'key': partKey,
                'last-modified': new Date().toJSON(),
                'content-md5': hexDigest,
                'content-length': size,
            };
            return metadata.putObjectMD(mpuBucketName, partKey, omVal, log,
                err => {
                    if (err) {
                        log.error('error putting object in mpu bucket', {
                            error: err,
                            method: 'objectPutPart::metadata.putObjectMD',
                        });
                        return next(err);
                    }
                    return next(null, hexDigest);
                });
        },
    ], (err, hexDigest) => {
        if (err) {
            log.error('error in object put part (upload part)', {
                error: err,
                method: 'objectPutPart',
            });
            return cb(err);
        }
        pushMetric('uploadPart', log, {
            authInfo,
            bucket: bucketName,
            newByteLength: size,
        });
        return cb(null, hexDigest);
    });
}
