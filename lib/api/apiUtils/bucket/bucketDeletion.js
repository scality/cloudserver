const assert = require('assert');
const async = require('async');
const { errors } = require('arsenal');

const abortMultipartUpload = require('../object/abortMultipartUpload');

const { splitter, oldSplitter, mpuBucketPrefix } =
    require('../../../../constants');
const metadata = require('../../../metadata/wrapper');
const kms = require('../../../kms/wrapper');
const deleteUserBucketEntry = require('./deleteUserBucketEntry');

function _deleteMPUbucket(destinationBucketName, log, cb) {
    const mpuBucketName =
        `${mpuBucketPrefix}${destinationBucketName}`;
    return metadata.deleteBucket(mpuBucketName, log, err => {
        // If the mpu bucket does not exist, just move on
        if (err && err.NoSuchBucket) {
            return cb();
        }
        return cb(err);
    });
}

function _deleteOngoingMPUs(authInfo, bucketName, mpus, log, cb) {
    async.mapLimit(mpus, 1, (mpu, next) => {
        const splitterChar = mpu.key.includes(oldSplitter) ?
            oldSplitter : splitter;
        // `overview${splitter}${objectKey}${splitter}${uploadId}
        const [, objectKey, uploadId] = mpu.key.split(splitterChar);
        abortMultipartUpload(authInfo, bucketName, objectKey, uploadId, log,
            (err, destBucket, partSizeSum) => next(err, partSizeSum));
    }, cb);
}
/**
 * deleteBucket - Delete bucket from namespace
 * @param {object} authInfo - authentication info
 * @param {object} bucketMD - bucket attributes/metadata
 * @param {string} bucketName - bucket in which objectMetadata is stored
 * @param {string} canonicalID - account canonicalID of requester
 * @param {object} log - Werelogs logger
 * @param {function} cb - callback from async.waterfall in bucketDelete
 * @return {undefined}
 */
function deleteBucket(authInfo, bucketMD, bucketName, canonicalID, log, cb) {
    log.trace('deleting bucket from metadata');
    assert.strictEqual(typeof bucketName, 'string');
    assert.strictEqual(typeof canonicalID, 'string');

    return async.waterfall([
        function checkForObjectsStep(next) {
            const params = { maxKeys: 1, listingType: 'DelimiterVersions' };
            // We list all the versions as we want to return BucketNotEmpty
            // error if there are any versions or delete markers in the bucket.
            // Works for non-versioned buckets as well since listing versions
            // includes null (non-versioned) objects in the result.
            return metadata.listObject(bucketName, params, log,
                (err, list) => {
                    if (err) {
                        log.error('error from metadata', { error: err });
                        return next(err);
                    }
                    const length = (list.Versions ? list.Versions.length : 0) +
                        (list.DeleteMarkers ? list.DeleteMarkers.length : 0);
                    log.debug('listing result', { length });
                    if (length) {
                        log.debug('bucket delete failed',
                            { error: errors.BucketNotEmpty });
                        return next(errors.BucketNotEmpty);
                    }
                    return next();
                });
        },

        function deleteMPUbucketStep(next) {
            const MPUBucketName = `${mpuBucketPrefix}${bucketName}`;
            // check to see if there are any mpu overview objects (so ignore
            // any orphaned part objects)
            return metadata.listObject(MPUBucketName, { prefix: 'overview' },
                log, (err, objectsListRes) => {
                    // If no shadow bucket ever created, no ongoing MPU's, so
                    // continue with deletion
                    if (err && err.NoSuchBucket) {
                        return next();
                    }
                    if (err) {
                        log.error('error from metadata', { error: err });
                        return next(err);
                    }
                    if (objectsListRes.Contents.length) {
                        return _deleteOngoingMPUs(authInfo, bucketName,
                            objectsListRes.Contents, log,
                            (err, partSizeSums) => {
                                if (err) {
                                    return next(err);
                                }
                                const totalPartSizeSum =
                                    partSizeSums.reduce((a, b) => a + b, 0);
                                log.trace('deleting shadow MPU bucket');
                                return _deleteMPUbucket(bucketName, log,
                                    err => next(err, totalPartSizeSum));
                            });
                    }
                    log.trace('deleting shadow MPU bucket');
                    return _deleteMPUbucket(bucketName, log, next);
                });
        },
        function addDeleteFlagStep(totalPartSizeSum, next) {
            log.trace('adding deleted attribute to bucket attributes');
            // Remove transient flag if any so never have both transient
            // and deleted flags.
            bucketMD.removeTransientFlag();
            bucketMD.addDeletedFlag();
            return metadata.updateBucket(bucketName, bucketMD, log,
                err => next(err, totalPartSizeSum));
        },
        function deleteUserBucketEntryStep(totalPartSizeSum, next) {
            log.trace('deleting bucket name from user bucket');
            return deleteUserBucketEntry(bucketName, canonicalID, log,
                err => next(err, totalPartSizeSum));
        },
    ],
    // eslint-disable-next-line prefer-arrow-callback
    function actualDeletionStep(err, totalPartSizeSum) {
        if (err) {
            return cb(err);
        }
        return metadata.deleteBucket(bucketName, log, err => {
            log.trace('deleting bucket from metadata');
            if (err) {
                return cb(err);
            }
            const serverSideEncryption = bucketMD.getServerSideEncryption();
            if (serverSideEncryption &&
                serverSideEncryption.algorithm === 'AES256') {
                const masterKeyId = serverSideEncryption.masterKeyId;
                return kms.destroyBucketKey(masterKeyId, log, cb);
            }
            return cb(null, totalPartSizeSum);
        });
    });
}

module.exports = deleteBucket;
