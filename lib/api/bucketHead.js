const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const { standardMetadataValidateBucket } = require('../metadata/metadataUtils');

const { pushMetric } = require('../utapi/utilities');
const monitoring = require('../utilities/monitoringHandler');
const { config } = require('../Config');

/**
 * Determine if bucket exists and if user has permission to access it
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to respond to http request
 *  with either error code or success
 * @return {undefined}
 */
function bucketHead(authInfo, request, log, callback) {
    log.info('=== BUCKETHEAD FUNCTION CALLED ===', { bucketName: request.bucketName });
    log.debug('processing request', { method: 'bucketHead' });
    const bucketName = request.bucketName;
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: request.apiMethods || 'bucketHead',
        request,
    };
    standardMetadataValidateBucket(metadataValParams, request.actionImplicitDenies, log, (err, bucket) => {
        log.info('bucketHead - INSIDE CALLBACK', { bucketName, hasError: !!err });

        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (err) {
            monitoring.promMetrics(
                        'HEAD', bucketName, err.code, 'headBucket');
            return callback(err, corsHeaders);
        }

        // PHASE 2: Post-Metadata Rate Limit Check (Authoritative)
        // Skip if pre-auth check already ran to avoid double-counting
        if (!request.rateLimitPreAuthChecked && config.rateLimiting && config.rateLimiting.enabled) {
            const { errors } = require('arsenal');
            const { checkBucketRateLimit } = require('./apiUtils/rateLimit');

            if (!checkBucketRateLimit(bucketName, bucket, config, log)) {
                log.debug('request throttled', {
                    bucketName,
                    method: 'bucketHead',
                });
                monitoring.promMetrics('HEAD', bucketName, 429, 'headBucket');
                return callback(config.rateLimiting.error || errors.SlowDown, corsHeaders);
            }
        } else if (request.rateLimitPreAuthChecked) {
            log.debug('skipping post-metadata check (pre-auth already checked)', {
                bucketName,
                method: 'bucketHead',
            });
        }

        pushMetric('headBucket', log, {
            authInfo,
            bucket: bucketName,
        });
        const headers = {
            'x-amz-bucket-region': bucket.getLocationConstraint(),
        };
        return callback(null, Object.assign(corsHeaders, headers));
    });
}

module.exports = bucketHead;
