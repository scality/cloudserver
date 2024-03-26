const { errors } = require('arsenal');

const { standardMetadataValidateBucket } = require('../metadata/metadataUtils');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');

/**
 * bucketGetQuota - Get the bucket quota
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
function bucketGetQuota(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketGetQuota' });
    const { bucketName, headers, method } = request;
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: request.apiMethods || 'bucketGetQuota',
        request,
    };

    return standardMetadataValidateBucket(metadataValParams, request.actionImplicitDenies, log, (err, bucket) => {
        const corsHeaders = collectCorsHeaders(headers.origin, method, bucket);
        if (err) {
            log.debug('error processing request', {
                error: err,
                method: 'bucketGetQuota',
            });
            return callback(err, null, corsHeaders);
        }
        const bucketQuota = bucket.getQuota();
        if (!bucketQuota) {
            log.debug('error processing request', {
                error: errors.NoSuchBucketQuota,
                method: 'bucketGetQuota',
            });
            return callback(errors.NoSuchBucketQuota, null,
                corsHeaders);
        }
        return callback(null, bucketQuota, corsHeaders);
    });
}

module.exports = bucketGetQuota;