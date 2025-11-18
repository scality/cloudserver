const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const { standardMetadataValidateBucket } = require('../metadata/metadataUtils');

const { pushMetric } = require('../utapi/utilities');
const monitoring = require('../utilities/monitoringHandler');
const { checkRateLimit } = require('./apiUtils/rateLimit/checker');
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
    log.debug('processing request', { method: 'bucketHead' });
    const bucketName = request.bucketName;

    // Rate limit check
    return checkRateLimit(bucketName, log, (err, rateLimited) => {
        if (err) {
            // This shouldn't happen (checker fails open), but handle it
            log.error('Rate limit check error', { error: err });
        }

        if (rateLimited) {
            log.info('Request rate limited', {
                method: 'bucketHead',
                bucketName,
            });
            // Return configured error (defaults to SlowDown)
            return callback(config.rateLimiting.error);
        }

        // Continue with normal bucket head flow
        const metadataValParams = {
            authInfo,
            bucketName,
            requestType: request.apiMethods || 'bucketHead',
            request,
        };
        return standardMetadataValidateBucket(
            metadataValParams,
            request.actionImplicitDenies,
            log,
            (bucketErr, bucket) => {
                const corsHeaders = collectCorsHeaders(
                    request.headers.origin,
                    request.method,
                    bucket
                );
                if (bucketErr) {
                    monitoring.promMetrics(
                        'HEAD',
                        bucketName,
                        bucketErr.code,
                        'headBucket'
                    );
                    return callback(bucketErr, corsHeaders);
                }
                pushMetric('headBucket', log, {
                    authInfo,
                    bucket: bucketName,
                });
                const headers = {
                    'x-amz-bucket-region': bucket.getLocationConstraint(),
                };
                return callback(null, Object.assign(corsHeaders, headers));
            }
        );
    });
}

module.exports = bucketHead;
