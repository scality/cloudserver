const { waterfall } = require('async');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const { standardMetadataValidateBucket } = require('../metadata/metadataUtils');
const metadata = require('../metadata/wrapper');
const { pushMetric } = require('../utapi/utilities');
const monitoring = require('../utilities/monitoringHandler');

/**
 * Bucket Update Quota - Update bucket quota
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
function bucketDeleteQuota(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketDeleteQuota' });

    const { bucketName } = request;
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: request.apiMethods || 'bucketDeleteQuota',
        request,
    };
    let bucket = null;
    return waterfall([
        next => standardMetadataValidateBucket(metadataValParams, request.actionImplicitDenies, log,
            (err, b) => {
                bucket = b;
                return next(err);
            }),
        next => {
            bucket.setQuota(0);
            metadata.updateBucket(bucket.getName(), bucket, log, err =>
                next(err));
        },
    ], err => {
        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (err) {
            log.debug('error processing request', {
                error: err,
                method: 'bucketDeleteQuota'
            });
            monitoring.promMetrics('DELETE', bucketName, err.code,
                'bucketDeleteQuota');
            return callback(err, err.code, corsHeaders);
        } else {
            monitoring.promMetrics(
                'DELETE', bucketName, '200', 'bucketDeleteQuota');
            pushMetric('bucketDeleteQuota', log, {
                authInfo,
                bucket: bucketName,
            });
        }
        return callback(null, 200, corsHeaders);
    });
}

module.exports = bucketDeleteQuota;
