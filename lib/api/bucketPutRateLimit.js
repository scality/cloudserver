const async = require('async');
const { errorInstances, errors, models } = require('@scality/arsenal');

const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const { config } = require('../Config');
const metadata = require('../metadata/wrapper');
const { standardMetadataValidateBucket } = require('../metadata/metadataUtils');
const { isRateLimitServiceUser } = require('./apiUtils/authorization/serviceUser');
const cache = require('./apiUtils/rateLimit/cache');

function parseRequestBody(requestBody, callback) {
    try {
        const jsonData = JSON.parse(requestBody);
        callback(null, jsonData);
    } catch {
        callback(errorInstances.InvalidArgument);
    }
}

function validateRateLimitConfig(requestConfig, callback) {
    const limit = requestConfig.RequestsPerSecond;
    if (Number.isNaN(limit) || !Number.isInteger(limit) || limit < 0) {
        return callback(
            errorInstances.InvalidArgument.customizeDescription('RequestsPerSecond must be a positive integer'),
        );
    }

    // Each node enforces limit/nodes locally; workers on a node share that quota
    const nodes = config.rateLimiting?.nodes || 1;

    if (limit > 0 && limit < nodes) {
        return callback(
            errorInstances.InvalidArgument.customizeDescription(
                `RequestsPerSecond (${limit}) must be >= nodes (${nodes}) or 0 (unlimited). ` +
                    'Each node enforces limit/nodes locally. ' +
                    `With limit less than ${nodes}, per-node rate would be less than 1 req/s, effectively blocking traffic.`,
            ),
        );
    }

    return callback(
        null,
        new models.RateLimitConfiguration({
            RequestsPerSecond: {
                Limit: limit,
            },
        }),
    );
}

/**
 * bucketPutRateLimit - create or update a bucket policy
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
function bucketPutRateLimit(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketPutRateLimit' });

    if (!isRateLimitServiceUser(authInfo)) {
        return callback(errors.AccessDenied);
    }

    const { bucketName } = request;
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: request.apiMethods || 'bucketPutRateLimit',
        request,
    };

    return async.waterfall(
        [
            next => parseRequestBody(request.post, next),
            (requestBody, next) => validateRateLimitConfig(requestBody, next),
            (limitConfig, next) =>
                standardMetadataValidateBucket(metadataValParams, request.actionImplicitDenies, log, (err, bucket) => {
                    if (err) {
                        return next(err, bucket);
                    }
                    return next(null, bucket, limitConfig);
                }),
            (bucket, limitConfig, next) => {
                bucket.setRateLimitConfiguration(limitConfig);
                metadata.updateBucket(bucket.getName(), bucket, log, err => next(err, bucket));
            },
        ],
        (err, bucket) => {
            const corsHeaders = collectCorsHeaders(request.headers.origin, request.method, bucket);
            if (err) {
                log.trace('error processing request', { error: err, method: 'bucketPutRateLimit' });
                return callback(err, corsHeaders);
            }
            // Invalidate cache so new limit takes effect immediately
            cache.deleteCachedConfig(cache.namespace.bucket, bucketName);
            log.debug('invalidated rate limit cache for bucket', { bucketName });
            // TODO: implement Utapi metric support
            return callback(null, corsHeaders);
        },
    );
}

module.exports = bucketPutRateLimit;
