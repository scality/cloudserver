const async = require('async');

const parseXML = require('../utilities/parseXML');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const getNotificationConfiguration = require('./apiUtils/bucket/getNotificationConfiguration');
const metadata = require('../metadata/wrapper');
const { standardMetadataValidateBucket } = require('../metadata/metadataUtils');
const { pushMetric } = require('../utapi/utilities');

/**
 * Bucket Put Notification - Create or update bucket notification configuration
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */

function bucketPutNotification(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketPutNotification' });

    const { bucketName } = request;
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: request.apiMethods || 'bucketPutNotification',
        request,
    };

    return async.waterfall([
        next => parseXML(request.post, log, next),
        (parsedXml, next) => {
            const notificationConfig = getNotificationConfiguration(parsedXml);
            const notifConfig = notificationConfig.error ? undefined : notificationConfig;
            process.nextTick(() => next(notificationConfig.error, notifConfig));
        },
        (notifConfig, next) => standardMetadataValidateBucket(metadataValParams, request.actionImplicitDenies, log,
            (err, bucket) => next(err, bucket, notifConfig)),
        (bucket, notifConfig, next) => {
            bucket.setNotificationConfiguration(notifConfig);
            metadata.updateBucket(bucket.getName(), bucket, log,
                err => next(err, bucket));
        },
    ], (err, bucket) => {
        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (err) {
            log.trace('error processing request', {
                error: err,
                method: 'bucketPutNotification',
            });
            return callback(err, corsHeaders);
        }
        pushMetric('putBucketNotification', log, {
            authInfo,
            bucket: bucketName,
        });
        return callback(null, corsHeaders);
    });
}

module.exports = bucketPutNotification;
