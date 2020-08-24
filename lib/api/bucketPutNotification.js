const async = require('async');

const parseXML = require('../utilities/parseXML');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const getNotificationConfiguration = require('./apiUtils/bucket/getNotificationConfiguration');
const metadata = require('../metadata/wrapper');
const { metadataValidateBucket } = require('../metadata/metadataUtils');
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

    const bucketName = request.bucketName;
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: 'bucketPutNotification',
    };

    return async.waterfall([
        next => parseXML(request.post, log, next),
        (parsedXml, next) => {
            const notificationConfig = getNotificationConfiguration(parsedXml);
            process.nextTick(() => {
                if (notificationConfig.error) {
                    return next(notificationConfig.error);
                }
                return next(null, notificationConfig);
            });
        },
        (notifConfig, next) => metadataValidateBucket(metadataValParams, log, (err, bucket) => {
            if (err) {
                return next(err, bucket);
            }
            return next(null, bucket, notifConfig);
        }),
        (bucket, notifConfig, next) => {
            bucket.setNotificationConfiguration(notifConfig);
            metadata.updateBucket(bucket.getName(), bucket, log,
                err => next(err, bucket));
        },
    ], (err, bucket) => {
        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (err) {
            log.trace('error processing request', { error: err,
                method: 'bucketPutNotification' });
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
