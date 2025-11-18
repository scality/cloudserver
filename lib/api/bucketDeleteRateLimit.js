const { errors } = require('arsenal');

const metadata = require('../metadata/wrapper');
const { standardMetadataValidateBucket } = require('../metadata/metadataUtils');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const { isRateLimitServiceUser } = require('./apiUtils/authorization/serviceUser');

/**
 * bucketDeleteRateLimit - Delete the bucket rate limit configuration
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
function bucketDeleteRateLimit(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketDeleteRateLimit' });

    if (!isRateLimitServiceUser(authInfo)) {
        return callback(errors.AccessDenied);
    }

    const { bucketName, headers, method } = request;
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: request.apiMethods || 'bucketDeleteRateLimit',
        request,
    };
    return standardMetadataValidateBucket(metadataValParams, request.actionImplicitDenies, log, (err, bucket) => {
        const corsHeaders = collectCorsHeaders(headers.origin, method, bucket);
        if (err) {
            log.debug('error processing request', {
                error: err,
                method: 'bucketDeleteRateLimit',
            });
            return callback(err, corsHeaders);
        }
        if (!bucket.getRateLimitConfiguration()) {
            log.trace('no existing bucket rate limit configuration', {
                method: 'bucketDeleteRateLimit',
            });
            // TODO: implement Utapi metric support
            return callback(null, corsHeaders);
        }
        log.trace('deleting bucket rate limit configuration in metadata');
        bucket.setRateLimitConfiguration(undefined);
        return metadata.updateBucket(bucketName, bucket, log, err => {
            if (err) {
                return callback(err, corsHeaders);
            }
            // TODO: implement Utapi metric support
            return callback(null, corsHeaders);
        });
    });
}

module.exports = bucketDeleteRateLimit;
