const async = require('async');
const { parseString } = require('xml2js');
const { errorInstances, errors, models } = require('arsenal');

const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const metadata = require('../metadata/wrapper');
const { isRateLimitServiceUser } = require('./apiUtils/authorization/serviceUser');
const { config } = require('../Config');

const RateLimitConfiguration = models.RateLimitConfiguration;

function parseRequestBody(requestBody, callback) {
    // Try JSON first
    let jsonData;
    try {
        jsonData = JSON.parse(requestBody);
        if (typeof jsonData !== 'object') {
            throw new Error('Invalid JSON - not an object');
        }
        // JSON succeeded - return immediately, do NOT try XML
        return callback(null, jsonData);
    } catch (jsonError) {
        // JSON failed - try XML
        parseString(requestBody, (xmlError, xmlData) => {
            if (xmlError) {
                return callback(errorInstances.InvalidArgument
                    .customizeDescription('Request body must be a JSON object'));
            }
            return callback(null, xmlData);
        });
    }
}

function validateRateLimitConfig(requestConfig, callback) {
    const limit = parseInt(requestConfig.RequestsPerSecond, 10);

    // Validate positive integer
    if (Number.isNaN(limit) || !Number.isInteger(limit) || limit <= 0) {
        return callback(errorInstances.InvalidArgument
            .customizeDescription('RequestsPerSecond must be a positive integer'));
    }

    // Validate minimum rate limit (must be >= number of nodes)
    const nodeCount = config.rateLimiting?.nodeCount || 1;
    if (limit < nodeCount) {
        return callback(errorInstances.InvalidArgument
            .customizeDescription(
                `RequestsPerSecond must be >= ${nodeCount} (number of CloudServer nodes)`
            ));
    }

    // Create RateLimitConfiguration model with flattened structure
    const rateLimitConfig = new RateLimitConfiguration({
        RequestsPerSecond: limit,
    });

    return callback(null, rateLimitConfig);
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

    const requestArn = authInfo.getArn();
    const configArn = config.rateLimiting?.serviceUserArn;
    log.debug('ARN comparison for rate limit authorization', {
        requestArn,
        configArn,
        match: requestArn === configArn,
    });

    if (!isRateLimitServiceUser(authInfo, log)) {
        log.warn('Access denied - ARN mismatch', { requestArn, configArn });
        return callback(errors.AccessDenied);
    }

    const { bucketName } = request;
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: request.apiMethods || 'bucketPutRateLimit',
        request,
    };

    return async.waterfall([
        next => parseRequestBody(request.post, next),
        (requestBody, next) => validateRateLimitConfig(requestBody, next),
        (limitConfig, next) => standardMetadataValidateBucket(metadataValParams, request.actionImplicitDenies, log,
            (err, bucket) => {
                if (err) {
                    return next(err, bucket);
                }
                return next(null, bucket, limitConfig);
            }),
        (bucket, limitConfig, next) => bucket.setRateLimitConfiguration(limitConfig)
            .then(() => metadata.updateBucket(bucket.getName(), bucket, log,
                err => next(err, bucket))),
    ], (err, bucket) => {
        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (err) {
            log.trace('error processing request',
                { error: err, method: 'bucketPutRateLimit' });
            return callback(err, corsHeaders);
        }
        // TODO: implement Utapi metric support
        return callback(null, corsHeaders);
    });
}

module.exports = bucketPutRateLimit;
