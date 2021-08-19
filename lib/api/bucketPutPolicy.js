const async = require('async');
const { errors, models } = require('arsenal');

const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const metadata = require('../metadata/wrapper');
const { metadataValidateBucket } = require('../metadata/metadataUtils');
const { validatePolicyResource } =
    require('./apiUtils/authorization/permissionChecks');
const { BucketPolicy } = models;

/**
 * _checkNotImplementedPolicy - some bucket policy features have not been
 * implemented and should return NotImplemented error
 * @param {string} policyString - string bucket policy
 * @return {boolean} - returns true if policy contains not implemented elements
 */
function _checkNotImplementedPolicy(policyString) {
    // bucket names and key names cannot include "", so including those
    // isolates not implemented keys
    return policyString.includes('"Condition"')
    || policyString.includes('"Service"')
    || policyString.includes('"Federated"');
}

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
        requestType: 'bucketPutPolicy',
        request,
    };

    return async.waterfall([
        next => {
            const bucketPolicy = new BucketPolicy(request.post);
            // if there was an error getting bucket policy,
            // returned policyObj will contain 'error' key
            process.nextTick(() => {
                const policyObj = bucketPolicy.getBucketPolicy();
                if (_checkNotImplementedPolicy(request.post)) {
                    const err = errors.NotImplemented.customizeDescription(
                        'Bucket policy contains element not yet implemented');
                    return next(err);
                }
                if (policyObj.error) {
                    const err = errors.MalformedPolicy.customizeDescription(
                        policyObj.error.description);
                    return next(err);
                }
                return next(null, policyObj);
            });
        },
        (bucketPolicy, next) => {
            process.nextTick(() => {
                if (!validatePolicyResource(bucketName, bucketPolicy)) {
                    return next(errors.MalformedPolicy.customizeDescription(
                        'Policy has invalid resource'));
                }
                return next(null, bucketPolicy);
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
        // TODO: implement Utapi metric support
        return callback(null, corsHeaders);
    });
}

module.exports = bucketPutPolicy;
