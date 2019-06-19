const { errors } = require('arsenal');

const { metadataValidateBucket } = require('../metadata/metadataUtils');
const { pushMetric } = require('../utapi/utilities');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');

/**
 * bucketGetLifecycle - Get the bucket lifecycle configuration
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
function bucketGetLifecycle(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketGetPolicy' });
    const { bucketName, headers, method } = request;
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: 'bucketOwnerAction',
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
                method: 'bucketGetLifecycle',
            });
            return callback(errors.NoSuchLifecycleConfiguration, null,
                corsHeaders);
        }
        pushMetric('getBucketPolicy', log, {
            authInfo,
            bucket: bucketName,
        });
        return callback(null, bucketPolicy, corsHeaders);
    });
}

module.exports = bucketGetLifecycle;
