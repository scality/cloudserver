const { errors } = require('arsenal');

const bucketShield = require('./apiUtils/bucket/bucketShield');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const { convertToXml } = require('./apiUtils/bucket/bucketCors');
const { isBucketAuthorized } = require('./apiUtils/authorization/aclChecks');
const metadata = require('../metadata/wrapper');
const { pushMetric } = require('../utapi/utilities');
const monitoring = require('../utilities/monitoringHandler');

const requestType = 'bucketOwnerAction';

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
    const canonicalID = authInfo.getCanonicalID();

    metadata.getBucket(bucketName, log, (err, bucket) => {
        if (err) {
            log.debug('metadata getbucket failed', { error: err });
            monitoring.promMetrics(
                'GET', bucketName, err.code, 'getBucketCors');
            return callback(err);
        }
        if (bucketShield(bucket, requestType)) {
            monitoring.promMetrics(
                'GET', bucketName, 404, 'getBucketCors');
            return callback(errors.NoSuchBucket);
        }
        log.trace('found bucket in metadata');
        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);

        if (!isBucketAuthorized(bucket, requestType, canonicalID)) {
            log.debug('access denied for user on bucket', {
                requestType,
                method: 'bucketGetCors',
            });
            monitoring.promMetrics(
                'GET', bucketName, 403, 'getBucketCors');
            return callback(errors.AccessDenied, null, corsHeaders);
        }

        const cors = bucket.getCors();
        if (!cors) {
            log.debug('cors configuration does not exist', {
                method: 'bucketGetCors',
            });
            monitoring.promMetrics(
                'GET', bucketName, 404, 'getBucketCors');
            return callback(errors.NoSuchCORSConfiguration, null, corsHeaders);
        }
        log.trace('converting cors configuration to xml');
        const xml = convertToXml(cors);

        pushMetric('getBucketCors', log, {
            authInfo,
            bucket: bucketName,
        });
        monitoring.promMetrics('GET', bucketName, '200', 'getBucketCors');
        return callback(null, xml, corsHeaders);
    });
}

module.exports = bucketGetCors;
