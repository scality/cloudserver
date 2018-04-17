const { errors } = require('arsenal');

const { metadataValidateBucket } = require('../metadata/metadataUtils');
const { pushMetric } = require('../utapi/utilities');
const { getReplicationConfigurationXML } =
    require('./apiUtils/bucket/getReplicationConfiguration');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const monitoring = require('../utilities/monitoringHandler');

/**
 * bucketGetReplication - Get the bucket replication configuration
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
function bucketGetReplication(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketGetReplication' });
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
                method: 'bucketGetReplication',
            });
            return callback(err, null, corsHeaders);
        }
        const replicationConfig = bucket.getReplicationConfiguration();
        if (!replicationConfig) {
            log.debug('error processing request', {
                error: errors.ReplicationConfigurationNotFoundError,
                method: 'bucketGetReplication',
            });
            return callback(errors.ReplicationConfigurationNotFoundError, null,
                corsHeaders);
        }
        const xml = getReplicationConfigurationXML(replicationConfig);
        pushMetric('getBucketReplication', log, {
            authInfo,
            bucket: bucketName,
        });
        monitoring.getRequest.inc();
        return callback(null, xml, corsHeaders);
    });
}

module.exports = bucketGetReplication;
