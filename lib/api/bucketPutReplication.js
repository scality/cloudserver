const { waterfall } = require('async');
const { errors } = require('arsenal');

const metadata = require('../metadata/wrapper');
const { metadataValidateBucket } = require('../metadata/metadataUtils');
const { pushMetric } = require('../utapi/utilities');
const { getReplicationConfiguration } =
    require('./apiUtils/bucket/getReplicationConfiguration');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const monitoring = require('../utilities/monitoringHandler');

// The error response when a bucket does not have versioning 'Enabled'.
const versioningNotEnabledError = errors.InvalidRequest.customizeDescription(
    'Versioning must be \'Enabled\' on the bucket to apply a replication ' +
    'configuration');

/**
 * bucketPutReplication - Create or update bucket replication configuration
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
function bucketPutReplication(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketPutReplication' });
    const { bucketName, post, headers, method } = request;
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: 'bucketOwnerAction',
    };
    return waterfall([
        // Validate the request XML and return the replication configuration.
        next => getReplicationConfiguration(post, log, next),
        // Check bucket user privileges and ensure versioning is 'Enabled'.
        (config, next) =>
            // TODO: Validate that destination bucket exists and has versioning.
            metadataValidateBucket(metadataValParams, log, (err, bucket) => {
                if (err) {
                    return next(err);
                }
                // Replication requires that versioning is 'Enabled'.
                if (!bucket.isVersioningEnabled(bucket)) {
                    return next(versioningNotEnabledError);
                }
                return next(null, config, bucket);
            }),
        // Set the replication configuration and update the bucket metadata.
        (config, bucket, next) => {
            bucket.setReplicationConfiguration(config);
            metadata.updateBucket(bucket.getName(), bucket, log, err =>
                next(err, bucket));
        },
    ], (err, bucket) => {
        const corsHeaders = collectCorsHeaders(headers.origin, method, bucket);
        if (err) {
            log.trace('error processing request', {
                error: err,
                method: 'bucketPutReplication',
            });
            return callback(err, corsHeaders);
        }
        pushMetric('putBucketReplication', log, {
            authInfo,
            bucket: bucketName,
        });
        monitoring.putRequest.inc();
        return callback(null, corsHeaders);
    });
}

module.exports = bucketPutReplication;
