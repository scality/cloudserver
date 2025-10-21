const async = require('async');
const { parseString } = require('xml2js');
const { errorInstances, errors } = require('arsenal');

const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const metadata = require('../metadata/wrapper');
const { standardMetadataValidateBucket } = require('../metadata/metadataUtils');
const { isRateLimitServiceUser } = require('./apiUtils/authorization/serviceUser');

function parseRequestBody(requestBody, callback) {
    try {
        const jsonData = JSON.parse(requestBody);
        if (typeof jsonData !== 'object') {
            throw new Error('Invalid JSON');
        }
        return callback(null, jsonData);
    } catch {
        return parseString(requestBody, (xmlError, xmlData) => {
            if (xmlError) {
                return callback(errorInstances.InvalidArgument
                    .customizeDescription('Request body must be a JSON object'));
            }
            return callback(null, xmlData);
        });
    }
}

function validateRateLimitConfig(config, callback) {
    const limit = parseInt(config.RequestsPerSecond, 10);
    if (Number.isNaN(limit) || !Number.isInteger(limit) || limit <= 0) {
        return callback(errorInstances.InvalidArgument
            .customizeDescription('RequestsPerSecond must be a positive integer'));
    }
    return callback(null, {
            RequestsPerSecond: limit,
    });
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
        (bucket, limitConfig, next) => {
            bucket.setRateLimitConfig(limitConfig);
            metadata.updateBucket(bucket.getName(), bucket, log,
                err => next(err, bucket));
        },
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
