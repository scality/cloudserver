const createKeyForUserBucket = require('./createKeyForUserBucket');
const { usersBucket, oldUsersBucket, splitter, oldSplitter } =
    require('../../../../constants');
const metadata = require('../../../metadata/wrapper');

function deleteUserBucketEntry(bucketName, canonicalID, log, cb) {
    log.trace('deleting bucket name from users bucket', { method:
        '_deleteUserBucketEntry' });
    const keyForUserBucket = createKeyForUserBucket(canonicalID, splitter,
        bucketName);
    metadata.deleteObjectMD(usersBucket, keyForUserBucket, {}, log, error => {
        // If the object representing the bucket is not in the
        // users bucket just continue
        if (error && error.NoSuchKey) {
            return cb(null);
        // BACKWARDS COMPATIBILITY: Remove this once no longer
        // have old user bucket format
        } else if (error && error.NoSuchBucket) {
            const keyForUserBucket2 = createKeyForUserBucket(canonicalID,
                oldSplitter, bucketName);
            return metadata.deleteObjectMD(oldUsersBucket, keyForUserBucket2,
                {}, log, error => {
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

module.exports = deleteUserBucketEntry;
