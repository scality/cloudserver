const { errors } = require('arsenal');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const { convertToXml } = require('./apiUtils/bucket/bucketCors');
const { standardMetadataValidateBucket } = require('../metadata/metadataUtils');
const { pushMetric } = require('../utapi/utilities');
const monitoring = require('../utilities/monitoringHandler');

const REQUEST_TYPE = 'bucketGetCors';
const METRICS_ACTION = 'getBucketCors';

/**
 * Bucket Get CORS - Get bucket cors configuration
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
function bucketGetCors(authInfo, request, log, callback) {
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
            monitoring.promMetrics('GET', bucketName, err.code, METRICS_ACTION);
            return callback(err, null, corsHeaders);
        }

        const cors = bucket.getCors();
        if (!cors) {
            log.debug('cors configuration does not exist', { method: REQUEST_TYPE });
            monitoring.promMetrics('GET', bucketName, 404, METRICS_ACTION);
            return callback(errors.NoSuchCORSConfiguration, null, corsHeaders);
        }
        log.trace('converting cors configuration to xml');
        const xml = convertToXml(cors);

        pushMetric(METRICS_ACTION, log, { authInfo, bucket: bucketName });
        monitoring.promMetrics('GET', bucketName, '200', METRICS_ACTION);
        return callback(null, xml, corsHeaders);
    });
}

module.exports = bucketGetCors;
