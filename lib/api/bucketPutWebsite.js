const async = require('async');
const { errors } = require('arsenal');

const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const metadata = require('../metadata/wrapper');
const { standardMetadataValidateBucket } = require('../metadata/metadataUtils');
const { parseWebsiteConfigXml } = require('./apiUtils/bucket/bucketWebsite');
const { pushMetric } = require('../utapi/utilities');
const monitoring = require('../utilities/monitoringHandler');

const REQUEST_TYPE = 'bucketPutWebsite';
const METRICS_ACTION = 'putBucketWebsite';

/**
 * Bucket Put Website - Create bucket website configuration
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
function bucketPutWebsite(authInfo, request, log, callback) {
    log.debug('processing request', { method: REQUEST_TYPE });
    const bucketName = request.bucketName;
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: request.apiMethod || REQUEST_TYPE,
        request,
    };

    if (!request.post) {
        monitoring.promMetrics('PUT', bucketName, 400, METRICS_ACTION);
        return callback(errors.MissingRequestBodyError);
    }

    return async.waterfall(
        [
            next => {
                log.trace('parsing website configuration');
                return parseWebsiteConfigXml(request.post, log, next);
            },
            (config, next) =>
                standardMetadataValidateBucket(metadataValParams, request.actionImplicitDenies, log, (err, bucket) => {
                    const corsHeaders = collectCorsHeaders(request.headers.origin, request.method, bucket);
                    if (err) {
                        monitoring.promMetrics('PUT', bucketName, err.code, METRICS_ACTION);
                        if (err?.is?.AccessDenied) {
                            return next(err, corsHeaders);
                        }
                        return next(err);
                    }

                    return next(null, bucket, config, corsHeaders);
                }),
            (bucket, config, corsHeaders, next) => {
                log.trace('updating bucket website configuration in metadata');
                bucket.setWebsiteConfiguration(config);
                return metadata.updateBucket(bucketName, bucket, log, err => {
                    next(err, corsHeaders);
                });
            },
        ],
        (err, corsHeaders) => {
            if (err) {
                log.trace('error processing request', { error: err, method: REQUEST_TYPE });
                monitoring.promMetrics('PUT', bucketName, err.code, METRICS_ACTION);
                return callback(err, corsHeaders);
            }

            pushMetric(METRICS_ACTION, log, { authInfo, bucket: bucketName });
            monitoring.promMetrics('PUT', bucketName, '200', METRICS_ACTION);
            return callback(null, corsHeaders);
        },
    );
}

module.exports = bucketPutWebsite;
