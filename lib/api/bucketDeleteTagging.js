const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const { metadataValidateBucket } = require('../metadata/metadataUtils');
const { pushMetric } = require('../utapi/utilities');
const metadata = require('../metadata/wrapper');
const util = require('node:util');
const monitoring = require('../utilities/metrics');

/**
 * Bucket Delete Tagging - Delete a bucket's Tagging
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
async function bucketDeleteTagging(authInfo, request, log, callback) {
    const bucketName = request.bucketName;
    let error = null;
    log.debug('processing request', { method: 'bucketDeleteTagging', bucketName });

    let bucket;
    const metadataValidateBucketPromise = util.promisify(metadataValidateBucket);
    let updateBucketPromise = util.promisify(metadata.updateBucket);
    // necessary to bind metadata as updateBucket calls 'this', causing undefined otherwise
    updateBucketPromise = updateBucketPromise.bind(metadata);
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: request.apiMethods || 'bucketDeleteTagging',
    };

    try {
        bucket = await metadataValidateBucketPromise(metadataValParams, request.actionImplicitDenies, log);
        bucket.setTags([]);
        // eslint-disable-next-line no-unused-expressions
        await updateBucketPromise(bucket.getName(), bucket, log);
        pushMetric('deleteBucketTagging', log, {
            authInfo,
            bucket: bucketName,
        });
        monitoring.promMetrics(
            'DELETE', bucketName, '200', 'deleteBucketTagging');
    } catch (err) {
        error = err;
        log.error('error processing request', { error: err,
            method: 'deleteBucketTagging', bucketName });
        monitoring.promMetrics('DELETE', bucketName, err.code,
            'deleteBucketTagging');
    }
    const corsHeaders = collectCorsHeaders(request.headers.origin,
        request.method, bucket);
    return callback(error, corsHeaders);
}

module.exports = bucketDeleteTagging;
