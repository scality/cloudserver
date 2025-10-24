const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const metadata = require('../metadata/wrapper');
const { standardMetadataValidateBucket } = require('../metadata/metadataUtils');
const { pushMetric } = require('../utapi/utilities');
const monitoring = require('../utilities/monitoringHandler');

const REQUEST_TYPE = 'bucketDeleteWebsite';
const METRICS_ACTION = 'deleteBucketWebsite';

function bucketDeleteWebsite(authInfo, request, log, callback) {
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
            monitoring.promMetrics('DELETE', bucketName, err.code, REQUEST_TYPE);
            if (err?.is?.AccessDenied) {
                return callback(err, corsHeaders);
            }
            return callback(err);
        }

        const websiteConfig = bucket.getWebsiteConfiguration();
        if (!websiteConfig) {
            log.trace('no existing website configuration', { method: REQUEST_TYPE });
            pushMetric(METRICS_ACTION, log, { authInfo, bucket: bucketName });
            return callback(null, corsHeaders);
        }

        log.trace('deleting website configuration in metadata');
        bucket.setWebsiteConfiguration(null);
        return metadata.updateBucket(bucketName, bucket, log, err => {
            if (err) {
                monitoring.promMetrics('DELETE', bucketName, err.code, METRICS_ACTION);
                return callback(err, corsHeaders);
            }
            pushMetric(METRICS_ACTION, log, { authInfo, bucket: bucketName });
            monitoring.promMetrics('DELETE', bucketName, '200', METRICS_ACTION);
            return callback(null, corsHeaders);
        });
    });
}

module.exports = bucketDeleteWebsite;
