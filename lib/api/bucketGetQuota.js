const { errors } = require('arsenal');
const { pushMetric } = require('../utapi/utilities');
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
    const xml = [];
    xml.push(
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<GetBucketQuota>',
        '<Name>', bucketName, '</Name>',
    );

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
            log.debug('bucket has no quota', {
                method: 'bucketGetQuota',
            });
            return callback(errors.NoSuchBucketQuota, null,
                corsHeaders);
        }
        xml.push('<Quota>', bucketQuota, '</Quota>',
            '</GetBucketQuota>');

        pushMetric('getBucketQuota', log, {
            authInfo,
            bucket: bucketName,
        });
        return callback(null, xml.join(''), corsHeaders);
    });
}

module.exports = bucketGetQuota;
