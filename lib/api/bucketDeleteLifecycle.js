const metadata = require('../metadata/wrapper');
const { metadataValidateBucket } = require('../metadata/metadataUtils');
const { pushMetric } = require('../utapi/utilities');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const monitoring = require('../utilities/monitoringHandler');

/**
 * bucketDeleteLifecycle - Delete the bucket Lifecycle configuration
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
function bucketDeleteLifecycle(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketDeleteLifecycle' });
    const { bucketName, headers, method } = request;
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: 'bucketDeleteLifecycle',
        request,
    };
    return metadataValidateBucket(metadataValParams, log, (err, bucket) => {
        const corsHeaders = collectCorsHeaders(headers.origin, method, bucket);
        if (err) {
            log.debug('error processing request', {
                error: err,
                method: 'bucketDeleteLifecycle',
            });
            monitoring.promMetrics(
                'DELETE', bucketName, err.code, 'deleteBucketLifecycle');
            return callback(err, corsHeaders);
        }
        if (!bucket.getLifecycleConfiguration()) {
            log.trace('no existing Lifecycle configuration', {
                method: 'bucketDeleteLifecycle',
            });
            pushMetric('deleteBucketLifecycle', log, {
                authInfo,
                bucket: bucketName,
            });
            return callback(null, corsHeaders);
        }
        log.trace('deleting Lifecycle configuration in metadata');
        bucket.setLifecycleConfiguration(null);
        return metadata.updateBucket(bucketName, bucket, log, err => {
            if (err) {
                monitoring.promMetrics(
                    'DELETE', bucketName, err.code, 'deleteBucketLifecycle');
                return callback(err, corsHeaders);
            }
            pushMetric('deleteBucketLifecycle', log, {
                authInfo,
                bucket: bucketName,
            });
            monitoring.promMetrics(
                'DELETE', bucketName, '200', 'deleteBucketLifecycle');
            return callback(null, corsHeaders);
        });
    });
}

module.exports = bucketDeleteLifecycle;
