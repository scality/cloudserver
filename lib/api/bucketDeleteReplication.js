const metadata = require('../metadata/wrapper');
const { metadataValidateBucket } = require('../metadata/metadataUtils');
const { pushMetric } = require('../utapi/utilities');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const monitoring = require('../utilities/monitoringHandler');

/**
 * bucketDeleteReplication - Delete the bucket replication configuration
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
function bucketDeleteReplication(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketDeleteReplication' });
    const { bucketName, headers, method } = request;
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: 'bucketDeleteReplication',
        request,
    };
    return metadataValidateBucket(metadataValParams, log, (err, bucket) => {
        const corsHeaders = collectCorsHeaders(headers.origin, method, bucket);
        if (err) {
            log.debug('error processing request', {
                error: err,
                method: 'bucketDeleteReplication',
            });
            monitoring.promMetrics(
                'DELETE', bucketName, err.code, 'deleteBucketReplication');
            return callback(err, corsHeaders);
        }
        if (!bucket.getReplicationConfiguration()) {
            log.trace('no existing replication configuration', {
                method: 'bucketDeleteReplication',
            });
            pushMetric('deleteBucketReplication', log, {
                authInfo,
                bucket: bucketName,
            });
            return callback(null, corsHeaders);
        }
        log.trace('deleting replication configuration in metadata');
        bucket.setReplicationConfiguration(null);
        return metadata.updateBucket(bucketName, bucket, log, err => {
            if (err) {
                monitoring.promMetrics(
                    'DELETE', bucketName, err.code, 'deleteBucketReplication');
                return callback(err, corsHeaders);
            }
            pushMetric('deleteBucketReplication', log, {
                authInfo,
                bucket: bucketName,
            });
            monitoring.promMetrics(
                'DELETE', bucketName, '200', 'deleteBucketReplication');
            return callback(null, corsHeaders);
        });
    });
}

module.exports = bucketDeleteReplication;
