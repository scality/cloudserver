const { errors } = require('arsenal');

const { metadataValidateBucket } = require('../metadata/metadataUtils');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');

/**
 * bucketGetPolicy - Get the bucket policy
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
function bucketGetPolicy(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketGetPolicy' });
    const { bucketName, headers, method } = request;
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: 'bucketGetPolicy',
        request,
    };

    return metadataValidateBucket(metadataValParams, log, (err, bucket) => {
        const corsHeaders = collectCorsHeaders(headers.origin, method, bucket);
        if (err) {
            log.debug('error processing request', {
                error: err,
                method: 'bucketGetPolicy',
            });
            return callback(err, null, corsHeaders);
        }
        const bucketPolicy = bucket.getBucketPolicy();
        if (!bucketPolicy) {
            log.debug('error processing request', {
                error: errors.NoSuchBucketPolicy,
                method: 'bucketGetPolicy',
            });
            return callback(errors.NoSuchBucketPolicy, null,
                corsHeaders);
        }
        // TODO: implement Utapi metric support
        // bucketPolicy needs to be JSON stringified on return for proper
        // parsing on return to caller function
        return callback(null, JSON.stringify(bucketPolicy), corsHeaders);
    });
}

module.exports = bucketGetPolicy;
