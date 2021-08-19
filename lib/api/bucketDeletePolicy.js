const metadata = require('../metadata/wrapper');
const { metadataValidateBucket } = require('../metadata/metadataUtils');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');

/**
 * bucketDeletePolicy - Delete the bucket policy
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
function bucketDeletePolicy(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketDeletePolicy' });
    const { bucketName, headers, method } = request;
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: 'bucketDeletePolicy',
        request,
    };
    return metadataValidateBucket(metadataValParams, log, (err, bucket) => {
        const corsHeaders = collectCorsHeaders(headers.origin, method, bucket);
        if (err) {
            log.debug('error processing request', {
                error: err,
                method: 'bucketDeletePolicy',
            });
            return callback(err, corsHeaders);
        }
        if (!bucket.getBucketPolicy()) {
            log.trace('no existing bucket policy', {
                method: 'bucketDeletePolicy',
            });
            // TODO: implement Utapi metric support
            return callback(null, corsHeaders);
        }
        log.trace('deleting bucket policy in metadata');
        bucket.setBucketPolicy(null);
        return metadata.updateBucket(bucketName, bucket, log, err => {
            if (err) {
                return callback(err, corsHeaders);
            }
            // TODO: implement Utapi metric support
            return callback(null, corsHeaders);
        });
    });
}

module.exports = bucketDeletePolicy;
