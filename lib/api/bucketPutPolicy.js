const async = require('async');
const { BucketPolicy } = require('arsenal').models;

const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const metadata = require('../metadata/wrapper');
const { metadataValidateBucket } = require('../metadata/metadataUtils');
const { pushMetric } = require('../utapi/utilities');
const { errors } = require('arsenal');

/**
 * bucketPutPolicy - create or update a bucket policy
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
function bucketPutPolicy(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketPutPolicy' });

    const { bucketName } = request;
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: 'bucketOwnerAction',
    };

    if (!process.env.BUCKET_POLICY) {
        return callback(errors.NotImplemented);
    }

    return async.waterfall([
        next => {
            const policyJSON = JSON.parse(request.post);
            const bucketPolicy = new BucketPolicy(policyJSON);
            // if there was an error getting bucket policy,
            // returned configObj will contain 'error' key
            process.nextTick(() => {
                const policyObj = bucketPolicy.getBucketPolicy();
                return next(policyObj.error || null, policyObj);
            });
        },
        (bucketPolicy, next) => metadataValidateBucket(metadataValParams, log,
            (err, bucket) => {
                if (err) {
                    return next(err, bucket);
                }
                return next(null, bucket, bucketPolicy);
            }),
        (bucket, bucketPolicy, next) => {
            bucket.setBucketPolicy(bucketPolicy);
            metadata.updateBucket(bucket.getName(), bucket, log,
                err => next(err, bucket));
        },
    ], (err, bucket) => {
        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (err) {
            log.trace('error processing request',
                { error: err, method: 'bucketPutPolicy' });
            return callback(err, corsHeaders);
        }
        pushMetric('putBucketPolicy', log, {
            authInfo,
            bucket: bucketName,
        });
        return callback(null, corsHeaders);
    });
}

module.exports = bucketPutPolicy;
