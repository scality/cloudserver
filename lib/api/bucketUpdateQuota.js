const { waterfall } = require('async');
const { errors } = require('arsenal');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const { standardMetadataValidateBucket } = require('../metadata/metadataUtils');
const metadata = require('../metadata/wrapper');
const { pushMetric } = require('../utapi/utilities');
const monitoring = require('../utilities/monitoringHandler');

/**
 * Bucket Update Quota - Update bucket quota
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
function updateBucketQuota(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketUpdateQuota' });

    const { bucketName } = request;
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: request.apiMethods || 'bucketUpdateQuota',
        request,
    };
    let bucket = null;
    return waterfall([
        next => standardMetadataValidateBucket(metadataValParams, request.actionImplicitDenies, log,
            (err, b) => {
                bucket = b;
                return next(err);
            }),
        next => {
            let requestBody;
            try {
                requestBody = JSON.parse(request.post);
            } catch (parseError) {
                return next(errors.InvalidArgument.customizeDescription('Invalid JSON format in request'));
            }
            if (typeof requestBody !== 'object' || Array.isArray(requestBody)) {
                return next(errors.InvalidArgument.customizeDescription('Request body must be a JSON object'));
            }
            const quota = parseInt(requestBody.quota, 10);
            if (isNaN(quota)) {
                return next(errors.InvalidArgument.customizeDescription('Quota Value should be a number'));
            }
            if (quota <= 0) {
                return next(errors.InvalidArgument.customizeDescription('Quota Value should be a positive number'));
            }
            // Update the bucket quota
            bucket.setQuota(quota);
            return metadata.updateBucket(bucket.getName(), bucket, log, err =>
                next(err));
        },
    ], err => {
        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (err) {
            log.debug('error processing request', {
                error: err,
                method: 'bucketUpdateQuota'
            });
            monitoring.promMetrics('PUT', bucketName, err.code,
                'updateBucketQuota');
            return callback(err, err.code, corsHeaders);
        } else {
            monitoring.promMetrics(
                'PUT', bucketName, '200', 'updateBucketQuota');
            pushMetric('updateBucketQuota', log, {
                authInfo,
                bucket: bucketName,
            });
        }
        return callback(null, 200, corsHeaders);
    });
}

module.exports = updateBucketQuota;
