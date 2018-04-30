const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const { metadataValidateBucket } = require('../metadata/metadataUtils');

const { pushMetric } = require('../utapi/utilities');
const monitoring = require('../utilities/monitoringHandler');

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
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: 'bucketHead',
    };
    metadataValidateBucket(metadataValParams, log, (err, bucket) => {
        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (err) {
            monitoring.promMetrics(
                        'HEAD', bucketName, err.code, 'headBucket');
            return callback(err, corsHeaders);
        }
        pushMetric('headBucket', log, {
            authInfo,
            bucket: bucketName,
        });
        monitoring.promMetrics(
                    'HEAD', bucketName, '200', 'headBucket');
        return callback(null, corsHeaders);
    });
}

module.exports = bucketHead;
