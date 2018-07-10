const { errors } = require('arsenal');

const bucketShield = require('./apiUtils/bucket/bucketShield');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const { isBucketAuthorized } = require('./apiUtils/authorization/aclChecks');
const metadata = require('../metadata/wrapper');
const { pushMetric } = require('../utapi/utilities');
const monitoring = require('../utilities/monitoringHandler');

const requestType = 'bucketOwnerAction';

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
    const canonicalID = authInfo.getCanonicalID();

    return metadata.getBucket(bucketName, log, (err, bucket) => {
        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (err) {
            log.debug('metadata getbucket failed', { error: err });
            monitoring.promMetrics('DELETE', bucketName, 400,
                'deleteBucketCors');
            return callback(err);
        }
        if (bucketShield(bucket, requestType)) {
            monitoring.promMetrics('DELETE', bucketName, 400,
                'deleteBucketCors');
            return callback(errors.NoSuchBucket);
        }
        log.trace('found bucket in metadata');

        if (!isBucketAuthorized(bucket, requestType, canonicalID)) {
            log.debug('access denied for user on bucket', {
                requestType,
                method: 'bucketDeleteCors',
            });
            monitoring.promMetrics('DELETE', bucketName, 403,
                'deleteBucketCors');
            return callback(errors.AccessDenied, corsHeaders);
        }

        const cors = bucket.getCors();
        if (!cors) {
            log.trace('no existing cors configuration', {
                method: 'bucketDeleteCors',
            });
            pushMetric('deleteBucketCors', log, {
                authInfo,
                bucket: bucketName,
            });
            return callback(null, corsHeaders);
        }

        log.trace('deleting cors configuration in metadata');
        bucket.setCors(null);
        return metadata.updateBucket(bucketName, bucket, log, err => {
            if (err) {
                monitoring.promMetrics('DELETE', bucketName, 400,
                    'deleteBucketCors');
                return callback(err, corsHeaders);
            }
            pushMetric('deleteBucketCors', log, {
                authInfo,
                bucket: bucketName,
            });
            monitoring.promMetrics(
                'DELETE', bucketName, '204', 'deleteBucketCors');
            return callback(err, corsHeaders);
        });
    });
}

module.exports = bucketDeleteCors;
