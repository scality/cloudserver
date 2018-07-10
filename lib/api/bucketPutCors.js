const crypto = require('crypto');
const async = require('async');
const { errors } = require('arsenal');

const bucketShield = require('./apiUtils/bucket/bucketShield');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const { isBucketAuthorized } = require('./apiUtils/authorization/aclChecks');
const metadata = require('../metadata/wrapper');
const { parseCorsXml } = require('./apiUtils/bucket/bucketCors');
const { pushMetric } = require('../utapi/utilities');
const monitoring = require('../utilities/monitoringHandler');

const requestType = 'bucketOwnerAction';

/**
 * Bucket Put Cors - Adds cors rules to bucket
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
function bucketPutCors(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketPutCors' });
    const bucketName = request.bucketName;
    const canonicalID = authInfo.getCanonicalID();

    if (!request.post) {
        log.debug('CORS xml body is missing',
        { error: errors.MissingRequestBodyError });
        monitoring.promMetrics('PUT', bucketName, 400, 'putBucketCors');
        return callback(errors.MissingRequestBodyError);
    }

    const md5 = crypto.createHash('md5')
        .update(request.post, 'utf8').digest('base64');
    if (md5 !== request.headers['content-md5']) {
        log.debug('bad md5 digest', { error: errors.BadDigest });
        monitoring.promMetrics('PUT', bucketName, 400, 'putBucketCors');
        return callback(errors.BadDigest);
    }

    if (parseInt(request.headers['content-length'], 10) > 65536) {
        const errMsg = 'The CORS XML document is limited to 64 KB in size.';
        log.debug(errMsg, { error: errors.MalformedXML });
        monitoring.promMetrics('PUT', bucketName, 400, 'putBucketCors');
        return callback(errors.MalformedXML.customizeDescription(errMsg));
    }

    return async.waterfall([
        function parseXmlBody(next) {
            log.trace('parsing cors rules');
            return parseCorsXml(request.post, log, next);
        },
        function getBucketfromMetadata(rules, next) {
            metadata.getBucket(bucketName, log, (err, bucket) => {
                if (err) {
                    log.debug('metadata getbucket failed', { error: err });
                    return next(err);
                }
                if (bucketShield(bucket, requestType)) {
                    return next(errors.NoSuchBucket);
                }
                log.trace('found bucket in metadata');
                // get corsHeaders before CORSConfiguration is updated
                const corsHeaders = collectCorsHeaders(request.headers.origin,
                    request.method, bucket);
                return next(null, bucket, rules, corsHeaders);
            });
        },
        function validateBucketAuthorization(bucket, rules, corsHeaders, next) {
            if (!isBucketAuthorized(bucket, requestType, canonicalID)) {
                log.debug('access denied for account on bucket', {
                    requestType,
                });
                return next(errors.AccessDenied, corsHeaders);
            }
            return next(null, bucket, rules, corsHeaders);
        },
        function updateBucketMetadata(bucket, rules, corsHeaders, next) {
            log.trace('updating bucket cors rules in metadata');
            bucket.setCors(rules);
            metadata.updateBucket(bucketName, bucket, log, err =>
                next(err, corsHeaders));
        },
    ], (err, corsHeaders) => {
        if (err) {
            log.trace('error processing request', { error: err,
                method: 'bucketPutCors' });
            monitoring.promMetrics('PUT', bucketName, err.code,
                'putBucketCors');
        }
        pushMetric('putBucketCors', log, {
            authInfo,
            bucket: bucketName,
        });
        monitoring.promMetrics('PUT', bucketName, '200', 'putBucketCors');
        return callback(err, corsHeaders);
    });
}

module.exports = bucketPutCors;
