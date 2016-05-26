import { errors } from 'arsenal';
import assert from 'assert';
import async from 'async';

import { logger } from '../../../utilities/logger';

import constants from '../../../../constants';
import metadata from '../../../metadata/wrapper';

const usersBucket = constants.usersBucket;
const oldUsersBucket = constants.oldUsersBucket;


function _deleteUserBucketEntry(bucketName, canonicalID, log, cb) {
    log.trace('deleting bucket name from users bucket', { method:
        '_deleteUserBucketEntry' });
    const keyForUserBucket = `${canonicalID}${constants.splitter}${bucketName}`;
    metadata.deleteObjectMD(usersBucket, keyForUserBucket, log, error => {
        // If the object representing the bucket is not in the
        // users bucket just continue
        if (error && error.NoSuchKey) {
            return cb(null);
        // BACKWARDS COMPATIBILITY: Remove this once no longer
        // have old user bucket format
        } else if (error && error.NoSuchBucket) {
            const keyForUserBucket2 =
                `${canonicalID}${constants.oldSplitter}${bucketName}`;
            return metadata.deleteObjectMD(oldUsersBucket, keyForUserBucket2,
                log, error => {
                    if (error && !error.NoSuchKey) {
                        log.error('from metadata while deleting user bucket',
                            { error });
                        return cb(error);
                    }
                    log.trace('deleted bucket from user bucket',
                    { method: '_deleteUserBucketEntry' });
                    return cb(null);
                });
        } else if (error) {
            log.error('from metadata while deleting user bucket', { error,
            method: '_deleteUserBucketEntry' });
            return cb(error);
        }
        log.trace('deleted bucket from user bucket', {
            method: '_deleteUserBucketEntry' });
        return cb(null);
    });
}


function _deleteMPUbucket(destinationBucketName, log, cb) {
    const mpuBucketName =
        `${constants.mpuBucketPrefix}${destinationBucketName}`;
    return metadata.deleteBucket(mpuBucketName, log, err => {
        // If the mpu bucket does not exist, just move on
        if (err && err.NoSuchBucket) {
            return cb();
        }
        return cb(err);
    });
}

/**
 * Invisibly finishes deleting a bucket that already has a deleted flag
 * by deleting the object in the users bucket representing the created bucket
 * and then deleting the bucket in metadata
 * @param {string} bucketName - name of bucket
 * @param {string} canonicalID - bucket owner's canonicalID
 * @return {undefined}
 */
export function invisiblyDelete(bucketName, canonicalID) {
    const log = logger.newRequestLogger();
    log.trace('deleting bucket with deleted flag invisibly', { bucketName });
    return _deleteUserBucketEntry(bucketName, canonicalID, log, err => {
        if (err) {
            log.error('error invisibly deleting bucket name from user bucket',
            { error: err });
            return log.end();
        }
        log.trace('deleted bucket name from user bucket');
        return metadata.deleteBucket(bucketName, log, error => {
            log.trace('deleting bucket from metadata',
            { method: 'invisiblyDelete' });
            if (error) {
                log.error('error deleting bucket from metadata', { error });
                return log.end();
            }
            log.trace('invisible deletion of bucket succeeded',
            { method: 'invisiblyDelete' });
            return log.end();
        });
    });
}

/**
 * deleteBucket - Delete bucket from namespace
 * @param {object} bucketMD - bucket attributes/metadata
 * @param {string} bucketName - bucket in which objectMetadata is stored
 * @param {string} canonicalID - account canonicalID of requester
 * @param {object} log - Werelogs logger
 * @param {function} cb - callback from async.waterfall in bucketDelete
 * @return {undefined}
 */
export function deleteBucket(bucketMD, bucketName, canonicalID, log, cb) {
    log.trace('deleting bucket from metadata');
    assert.strictEqual(typeof bucketName, 'string');
    assert.strictEqual(typeof canonicalID, 'string');

    return async.waterfall([
        function checkForObjectsStep(next) {
            return metadata.listObject(bucketName, null, null, null, 1, log,
                (err, objectsListRes) => {
                    if (err) {
                        log.error('error from metadata', { error: err });
                        return next(err);
                    }
                    if (objectsListRes.Contents.length) {
                        log.debug('bucket delete failed',
                            { error: errors.BucketNotEmpty });
                        return next(errors.BucketNotEmpty);
                    }
                    return next();
                });
        },
        // Note: This does not mirror AWS behavior.  AWS will allow a user to
        // delete a bucket even if there are ongoing multipart uploads.
        function deleteMPUbucketStep(next) {
            const MPUBucketName = `${constants.mpuBucketPrefix}${bucketName}`;
            return metadata.listObject(MPUBucketName, null, null, null,
                1, log, (err, objectsListRes) => {
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
                        log.debug('bucket delete failed',
                            { error: errors.MPUinProgress });
                        // Return non-AWS standard error
                        // regarding ongoing MPUs so user
                        // understands what is occurring
                        return next(errors.MPUinProgress);
                    }
                    log.trace('deleting shadow MPU bucket');
                    return _deleteMPUbucket(bucketName, log, next);
                });
        },
        function addDeleteFlagStep(next) {
            log.trace('adding deleted attribute to bucket attributes');
            // Remove transient flag if any so never have both transient
            // and deleted flags.
            bucketMD.removeTransientFlag();
            bucketMD.addDeletedFlag();
            return metadata.updateBucket(bucketName, bucketMD, log, next);
        },
        function deleteUserBucketEntryStep(next) {
            log.trace('deleting bucket name from user bucket');
            return _deleteUserBucketEntry(bucketName, canonicalID, log, next);
        },
    ],
    function actualDeletionStep(err) {
        if (err) {
            return cb(err);
        }
        return metadata.deleteBucket(bucketName, log, err => {
            log.trace('deleting bucket from metadata');
            return cb(err);
        });
    });
}
