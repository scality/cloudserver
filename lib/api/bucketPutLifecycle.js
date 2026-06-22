const { promisify } = require('util');
const uuid = require('uuid').v4;
const LifecycleConfiguration = require('arsenal').models.LifecycleConfiguration;

const config = require('../Config').config;
const parseXML = require('../utilities/parseXML');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const metadata = require('../metadata/wrapper');
const { standardMetadataValidateBucket } = require('../metadata/metadataUtils');
const { pushMetric } = require('../utapi/utilities');
const monitoring = require('../utilities/monitoringHandler');

/**
 * Bucket Put Lifecycle - Create or update bucket lifecycle configuration
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} [callback] - callback to server
 * @return {Promise} - resolves with the CORS response headers
 */
async function bucketPutLifecycle(authInfo, request, log, callback) {
    if (callback) {
        return bucketPutLifecycle(authInfo, request, log)
            .then(corsHeaders => callback(null, corsHeaders))
            .catch(err => callback(err, err.additionalResHeaders));
    }

    log.debug('processing request', { method: 'bucketPutLifecycle' });

    const { bucketName } = request;
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: request.apiMethods || 'bucketPutLifecycle',
        request,
    };

    let bucket;
    try {
        const parsedXml = await promisify(parseXML)(request.post, log);
        const lcConfig = new LifecycleConfiguration(parsedXml, log, config).getLifecycleConfiguration();
        if (lcConfig.error) {
            throw lcConfig.error;
        }
        bucket = await promisify(standardMetadataValidateBucket)(metadataValParams, request.actionImplicitDenies, log);
        if (!bucket.getUid()) {
            bucket.setUid(uuid());
        }
        bucket.setLifecycleConfiguration(lcConfig);
        await promisify(metadata.updateBucket).call(metadata, bucket.getName(), bucket, log);
    } catch (err) {
        log.trace('error processing request', { error: err, method: 'bucketPutLifecycle' });
        monitoring.promMetrics('PUT', bucketName, err.code, 'putBucketLifecycle');
        err.additionalResHeaders ||= collectCorsHeaders(request.headers.origin, request.method, bucket);
        throw err;
    }

    pushMetric('putBucketLifecycle', log, {
        authInfo,
        bucket: bucketName,
    });
    monitoring.promMetrics('PUT', bucketName, '200', 'putBucketLifecycle');
    return collectCorsHeaders(request.headers.origin, request.method, bucket);
}

module.exports = bucketPutLifecycle;
