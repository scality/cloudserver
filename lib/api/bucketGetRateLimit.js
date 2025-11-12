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

    // if (!isRateLimitServiceUser(authInfo, log)) {
    //     return callback(errors.AccessDenied);
    // }

    const bucketName = request.bucketName;
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: request.apiMethods || 'bucketGetRateLimit',
        request,
    };

    return standardMetadataValidateBucket(metadataValParams, request.actionImplicitDenies, log, (err, bucket) => {
        const corsHeaders = collectCorsHeaders(request.headers.origin, request.method, bucket);
        if (err) {
            log.debug('error processing request', {
                error: err,
                method: 'bucketGetRateLimit',
            });
            return callback(err, null, corsHeaders);
        }

        const rateLimitConfig = bucket.getRateLimitConfiguration();
        const limit = rateLimitConfig?.getRequestsPerSecondLimit();

        if (!rateLimitConfig || limit === undefined) {
            log.debug('error processing request', {
                error: errors.NoSuchRateLimitConfiguration,
                method: 'bucketGetRateLimit',
            });
            return callback(errors.NoSuchRateLimitConfiguration, null,
                corsHeaders);
        }

        // Return flattened structure matching API spec: {"RequestsPerSecond": 1000}
        const response = {
            RequestsPerSecond: limit,
        };

        return callback(null, JSON.stringify(response), corsHeaders);
    });
}

module.exports = bucketGetRateLimit;
