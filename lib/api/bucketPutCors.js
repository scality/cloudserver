const async = require('async');
const { errors, errorInstances } = require('arsenal');

const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const metadata = require('../metadata/wrapper');
const { standardMetadataValidateBucket } = require('../metadata/metadataUtils');
const { parseCorsXml } = require('./apiUtils/bucket/bucketCors');
const { pushMetric } = require('../utapi/utilities');
const monitoring = require('../utilities/monitoringHandler');

const REQUEST_TYPE = 'bucketPutCors';
const METRICS_ACTION = 'putBucketCors';

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
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: request.apiMethod || REQUEST_TYPE,
        request,
    };

    if (!request.post) {
        log.debug('CORS xml body is missing', { error: errors.MissingRequestBodyError });
        monitoring.promMetrics('PUT', bucketName, 400, METRICS_ACTION);
        return callback(errors.MissingRequestBodyError);
    }

    if (parseInt(request.headers['content-length'], 10) > 65536) {
        const errMsg = 'The CORS XML document is limited to 64 KB in size.';
        log.debug(errMsg, { error: errors.MalformedXML });
        monitoring.promMetrics('PUT', bucketName, 400, METRICS_ACTION);
        return callback(errorInstances.MalformedXML.customizeDescription(errMsg));
    }

    return async.waterfall(
        [
            next => {
                log.trace('parsing cors rules');
                return parseCorsXml(request.post, log, next);
            },
            (rules, next) =>
                standardMetadataValidateBucket(metadataValParams, request.actionImplicitDenies, log, (err, bucket) => {
                    const corsHeaders = collectCorsHeaders(request.headers.origin, request.method, bucket);
                    if (err) {
                        monitoring.promMetrics('PUT', bucketName, err.code, METRICS_ACTION);
                        if (err?.is?.AccessDenied) {
                            return next(err, corsHeaders);
                        }
                        return next(err);
                    }

                    return next(null, bucket, rules, corsHeaders);
                }),
            (bucket, rules, corsHeaders, next) => {
                bucket.setCors(rules);
                return metadata.updateBucket(bucketName, bucket, log, err => next(err, corsHeaders));
            },
        ],
        (err, corsHeaders) => {
            if (err) {
                log.trace('error processing request', { error: err, method: 'bucketPutCors' });
                monitoring.promMetrics('PUT', bucketName, err.code, METRICS_ACTION);
                return callback(err, corsHeaders);
            }
            pushMetric(METRICS_ACTION, log, { authInfo, bucket: bucketName });
            monitoring.promMetrics('PUT', bucketName, '200', METRICS_ACTION);
            return callback(null, corsHeaders);
        },
    );
}

module.exports = bucketPutCors;
