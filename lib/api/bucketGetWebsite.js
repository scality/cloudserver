const { errors } = require('arsenal');

const { convertToXml } = require('./apiUtils/bucket/bucketWebsite');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const { standardMetadataValidateBucket } = require('../metadata/metadataUtils');
const { pushMetric } = require('../utapi/utilities');
const monitoring = require('../utilities/monitoringHandler');

const REQUEST_TYPE = 'bucketGetWebsite';
const METRICS_ACTION = 'getBucketWebsite';

/**
 * Bucket Get Website - Get bucket website configuration
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
function bucketGetWebsite(authInfo, request, log, callback) {
    const bucketName = request.bucketName;
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: request.apiMethod || REQUEST_TYPE,
        request,
    };

    return standardMetadataValidateBucket(metadataValParams, request.actionImplicitDenies, log, (err, bucket) => {
        const corsHeaders = collectCorsHeaders(request.headers.origin, request.method, bucket);
        if (err) {
            monitoring.promMetrics('GET', bucketName, err.code, METRICS_ACTION);
            return callback(err, null, corsHeaders);
        }

        const websiteConfig = bucket.getWebsiteConfiguration();
        if (!websiteConfig) {
            log.debug('bucket website configuration does not exist', { method: REQUEST_TYPE });
            monitoring.promMetrics('GET', bucketName, 404, METRICS_ACTION);
            return callback(errors.NoSuchWebsiteConfiguration, null, corsHeaders);
        }
        log.trace('converting website configuration to xml');
        const xml = convertToXml(websiteConfig);

        pushMetric(METRICS_ACTION, log, { authInfo, bucket: bucketName });
        monitoring.promMetrics('GET', bucketName, '200', METRICS_ACTION);
        return callback(null, xml, corsHeaders);
    });
}

module.exports = bucketGetWebsite;
