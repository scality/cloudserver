const async = require('async');
const { errors } = require('arsenal');

const bucketShield = require('./apiUtils/bucket/bucketShield');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const { isBucketAuthorized } = require('./apiUtils/authorization/aclChecks');
const metadata = require('../metadata/wrapper');
const { parseWebsiteConfigXml } = require('./apiUtils/bucket/bucketWebsite');
const { pushMetric } = require('../utapi/utilities');

const requestType = 'bucketOwnerAction';

/**
 * Bucket Put Website - Create bucket website configuration
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
function bucketPutWebsite(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketPutWebsite' });
    const bucketName = request.bucketName;
    const canonicalID = authInfo.getCanonicalID();

    if (!request.post) {
        return callback(errors.MissingRequestBodyError);
    }
    return async.waterfall([
        function parseXmlBody(next) {
            log.trace('parsing website configuration');
            return parseWebsiteConfigXml(request.post, log, next);
        },
        function getBucketfromMetadata(config, next) {
            metadata.getBucket(bucketName, log, (err, bucket) => {
                if (err) {
                    log.debug('metadata getbucket failed', { error: err });
                    return next(err);
                }
                if (bucketShield(bucket, requestType)) {
                    return next(errors.NoSuchBucket);
                }
                log.trace('found bucket in metadata');
                return next(null, bucket, config);
            });
        },
        function validateBucketAuthorization(bucket, config, next) {
            if (!isBucketAuthorized(bucket, requestType, canonicalID)) {
                log.debug('access denied for user on bucket', {
                    requestType,
                    method: 'bucketPutWebsite',
                });
                return next(errors.AccessDenied, bucket);
            }
            return next(null, bucket, config);
        },
        function updateBucketMetadata(bucket, config, next) {
            log.trace('updating bucket website configuration in metadata');
            bucket.setWebsiteConfiguration(config);
            metadata.updateBucket(bucketName, bucket, log, err => {
                next(err, bucket);
            });
        },
    ], (err, bucket) => {
        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (err) {
            log.trace('error processing request', { error: err,
                method: 'bucketPutWebsite' });
        } else {
            pushMetric('putBucketWebsite', log, {
                authInfo,
                bucket: bucketName,
            });
        }
        return callback(err, corsHeaders);
    });
}

module.exports = bucketPutWebsite;
