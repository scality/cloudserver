const { errors } = require('arsenal');

const { standardMetadataValidateBucket } = require('../metadata/metadataUtils');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const { isRateLimitServiceUser } = require('./apiUtils/authorization/serviceUser');

/**
 * bucketGetRateLimit - Get the bucket rate limit config
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
function bucketGetRateLimit(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketGetRateLimit' });

    if (!isRateLimitServiceUser(authInfo)) {
        return callback(errors.AccessDenied);
    }

    const { bucketName, headers, method } = request;
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: request.apiMethods || 'bucketGetRateLimit',
        request,
    };

    return standardMetadataValidateBucket(metadataValParams, request.actionImplicitDenies, log, (err, bucket) => {
        const corsHeaders = collectCorsHeaders(headers.origin, method, bucket);
        if (err) {
            log.debug('error processing request', {
                error: err,
                method: 'bucketGetRateLimit',
            });
            return callback(err, null, corsHeaders);
        }

        const rateLimitConfig = bucket.getRateLimitConfig();
        if (!rateLimitConfig) {
            log.debug('error processing request', {
                error: errors.NoSuchBucketRateLimit,
                method: 'bucketGetRateLimit',
            });
            return callback(errors.NoSuchBucketRateLimit, null,
                corsHeaders);
        }

        return callback(null, JSON.stringify(rateLimitConfig), corsHeaders);
    });
}

module.exports = bucketGetRateLimit;
