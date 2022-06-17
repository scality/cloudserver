const { waterfall } = require('async');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const { metadataValidateBucket } = require('../metadata/metadataUtils');
const { pushMetric } = require('../utapi/utilities');
const monitoring = require('../utilities/monitoringHandler');
const metadata = require('../metadata/wrapper');

/**
 * Bucket Delete Tagging - Delete a bucket's Tagging
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
function bucketDeleteTagging(authInfo, request, log, callback) {
    const bucketName = request.bucketName;
    log.debug('processing request', { method: 'bucketDeleteTagging', bucketName });

    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: 'bucketDeleteTagging',
    };

    let bucket = null;
    return waterfall([
        next => metadataValidateBucket(metadataValParams, log,
            (err, b) => {
                bucket = b;
                bucket.setTags([]);
                return next(err);
            }),
        next => metadata.updateBucket(bucket.getName(), bucket, log, next),
    ], err => {
        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (err) {
            log.error('error processing request', {
                error: err,
                method: 'deleteBucketTagging',
                bucketName
            });
            monitoring.promMetrics('DELETE', bucketName, err.code,
                'deleteBucketTagging');
            return callback(err, corsHeaders);
        }
        pushMetric('deleteBucketTagging', log, {
            authInfo,
            bucket: bucketName,
        });
        monitoring.promMetrics(
            'DELETE', bucketName, '200', 'deleteBucketTagging');
        return callback(err, corsHeaders);
    });
}

module.exports = bucketDeleteTagging;
