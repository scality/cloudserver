const { errors } = require('arsenal');
const LifecycleConfiguration =
    require('arsenal').models.LifecycleConfiguration;

const { metadataValidateBucket } = require('../metadata/metadataUtils');
const { pushMetric } = require('../utapi/utilities');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');

/**
 * bucketGetLifecycle - Get the bucket lifecycle configuration
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
function bucketGetLifecycle(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketGetLifecycle' });
    const { bucketName, headers, method } = request;
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: 'bucketGetLifecycle',
        request,
    };
    return metadataValidateBucket(metadataValParams, request.actionImplicitDenies, log, (err, bucket) => {
        const corsHeaders = collectCorsHeaders(headers.origin, method, bucket);
        if (err) {
            log.debug('error processing request', {
                error: err,
                method: 'bucketGetLifecycle',
            });
            return callback(err, null, corsHeaders);
        }
        const lifecycleConfig = bucket.getLifecycleConfiguration();
        if (!lifecycleConfig) {
            log.debug('error processing request', {
                error: errors.NoSuchLifecycleConfiguration,
                method: 'bucketGetLifecycle',
            });
            return callback(errors.NoSuchLifecycleConfiguration, null,
                corsHeaders);
        }
        const xml = LifecycleConfiguration.getConfigXml(lifecycleConfig);
        pushMetric('getBucketLifecycle', log, {
            authInfo,
            bucket: bucketName,
        });
        return callback(null, xml, corsHeaders);
    });
}

module.exports = bucketGetLifecycle;
