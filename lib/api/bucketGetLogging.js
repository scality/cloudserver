const { standardMetadataValidateBucket } = require('../metadata/metadataUtils');
const { checkExpectedBucketOwner } = require('./apiUtils/authorization/bucketOwner');
const monitoring = require('../utilities/monitoringHandler');
const { waterfall } = require('async');

const BucketLoggingStatusNotFoundBody = '<?xml version="1.0" encoding="UTF-8"?>\n' +
    '<BucketLoggingStatus xmlns="http://doc.s3.amazonaws.com/2006-03-01" />';

function bucketGetLogging(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketGetLogging' });

    const bucketName = request.bucketName;
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: request.apiMethods || 'bucketGetLogging',
        request,
    };

    return waterfall([
        next => standardMetadataValidateBucket(metadataValParams, request.actionImplicitDenies, log, (err, bucket) => {
            if (err) {
                return next(err);
            }

            return next(null, bucket);
        }),
        (bucket, next) => checkExpectedBucketOwner(request.headers, bucket, log, err => {
            if (err) {
                return next(err);
            }

            return next(null, bucket);
        }),
        (bucket, next) => {
            const bucketLoggingStatus = bucket.getBucketLoggingStatus();
            if (!bucketLoggingStatus) {
                return next(null, BucketLoggingStatusNotFoundBody);
            }

            return next(null, bucketLoggingStatus.toXML());
        }
    ], (err, body) => {
        if (err) {
            log.trace('error processing request', { error: err, method: 'bucketGetLogging' });
            monitoring.promMetrics('GET', bucketName, err.code, 'bucketGetLogging');
            return callback(err);
        }

        monitoring.promMetrics('GET', bucketName, '200', 'bucketGetLogging');
        return callback(null, body);
    });
}

module.exports = bucketGetLogging;
