const { errors } = require('arsenal');

const bucketShield = require('./apiUtils/bucket/bucketShield');
const { convertToXml } = require('./apiUtils/bucket/bucketWebsite');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const { isBucketAuthorized } =
    require('./apiUtils/authorization/permissionChecks');
const metadata = require('../metadata/wrapper');
const { pushMetric } = require('../utapi/utilities');
const monitoring = require('../utilities/monitoringHandler');

const requestType = 'bucketGetWebsite';

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
    const canonicalID = authInfo.getCanonicalID();

    metadata.getBucket(bucketName, log, (err, bucket) => {
        if (err) {
            log.debug('metadata getbucket failed', { error: err });
            monitoring.promMetrics(
                'GET', bucketName, err.code, 'getBucketWebsite');
            return callback(err);
        }
        if (bucketShield(bucket, requestType)) {
            monitoring.promMetrics(
                'GET', bucketName, 404, 'getBucketWebsite');
            return callback(errors.NoSuchBucket);
        }
        log.trace('found bucket in metadata');

        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (!isBucketAuthorized(bucket, requestType, canonicalID, authInfo, log, request)) {
            log.debug('access denied for user on bucket', {
                requestType,
                method: 'bucketGetWebsite',
            });
            monitoring.promMetrics(
                'GET', bucketName, 403, 'getBucketWebsite');
            return callback(errors.AccessDenied, null, corsHeaders);
        }

        const websiteConfig = bucket.getWebsiteConfiguration();
        if (!websiteConfig) {
            log.debug('bucket website configuration does not exist', {
                method: 'bucketGetWebsite',
            });
            monitoring.promMetrics(
                'GET', bucketName, 404, 'getBucketWebsite');
            return callback(errors.NoSuchWebsiteConfiguration, null,
                corsHeaders);
        }
        log.trace('converting website configuration to xml');
        const xml = convertToXml(websiteConfig);

        pushMetric('getBucketWebsite', log, {
            authInfo,
            bucket: bucketName,
        });
        monitoring.promMetrics(
            'GET', bucketName, '200', 'getBucketWebsite');
        return callback(null, xml, corsHeaders);
    });
}

module.exports = bucketGetWebsite;
