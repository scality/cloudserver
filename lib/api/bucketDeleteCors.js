const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const metadata = require('../metadata/wrapper');
const { standardMetadataValidateBucket } = require('../metadata/metadataUtils');
const { pushMetric } = require('../utapi/utilities');
const monitoring = require('../utilities/monitoringHandler');

const REQUEST_TYPE = 'bucketDeleteCors';
const METRICS_ACTION = 'deleteBucketCors';

/**
 * Bucket Delete CORS - Delete bucket cors configuration
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
function bucketDeleteCors(authInfo, request, log, callback) {
    const bucketName = request.bucketName;
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: REQUEST_TYPE,
        request,
    };

    return standardMetadataValidateBucket(metadataValParams, request.actionImplicitDenies, log, (err, bucket) => {
        const corsHeaders = collectCorsHeaders(request.headers.origin, request.method, bucket);
        if (err) {
            monitoring.promMetrics('DELETE', bucketName, err.code, METRICS_ACTION);
            if (err?.is?.AccessDenied) {
                return callback(err, corsHeaders);
            }
            return callback(err);
        }

        const cors = bucket.getCors();
        if (!cors) {
            log.trace('no existing cors configuration', { method: REQUEST_TYPE });
            pushMetric(METRICS_ACTION, log, { authInfo, bucket: bucketName });
            return callback(null, corsHeaders);
        }

        log.trace('deleting cors configuration in metadata');
        bucket.setCors(null);
        return metadata.updateBucket(bucketName, bucket, log, err => {
            if (err) {
                monitoring.promMetrics('DELETE', bucketName, err.code, METRICS_ACTION);
                return callback(err, corsHeaders);
            }
            pushMetric(METRICS_ACTION, log, { authInfo, bucket: bucketName });
            monitoring.promMetrics('DELETE', bucketName, '204', METRICS_ACTION);
            return callback(null, corsHeaders);
        });
    });
}

module.exports = bucketDeleteCors;
