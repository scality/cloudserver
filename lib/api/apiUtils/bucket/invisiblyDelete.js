const logger = require('../../../utilities/logger');
const deleteUserBucketEntry = require('./deleteUserBucketEntry');
const metadata = require('../../../metadata/wrapper');

/**
 * Invisibly finishes deleting a bucket that already has a deleted flag
 * by deleting the object in the users bucket representing the created bucket
 * and then deleting the bucket in metadata
 * @param {string} bucketName - name of bucket
 * @param {string} canonicalID - bucket owner's canonicalID
 * @return {undefined}
 */
function invisiblyDelete(bucketName, canonicalID) {
    const log = logger.newRequestLogger();
    log.trace('deleting bucket with deleted flag invisibly', { bucketName });
    return deleteUserBucketEntry(bucketName, canonicalID, log, err => {
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

module.exports = invisiblyDelete;
