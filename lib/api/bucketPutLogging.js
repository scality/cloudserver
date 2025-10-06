const { waterfall } = require('async');
const { standardMetadataValidateBucket } = require('../metadata/metadataUtils');
const { checkExpectedBucketOwner } = require('./apiUtils/authorization/bucketOwner');
const BucketLoggingStatus = require('arsenal').models.BucketLoggingStatus;
const metadata = require('../metadata/wrapper');
const monitoring = require('../utilities/monitoringHandler');
const { errorInstances } = require('arsenal');

function bucketPutLogging(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketPutLogging' });

    const bucketName = request.bucketName;
    const parsed = BucketLoggingStatus.fromXML(request.post);
    if (parsed.error) {
        log.trace('error processing request', { error: parsed.error, method: 'bucketPutLogging' });
        monitoring.promMetrics('PUT', bucketName, parsed.error.arsenalError, 'bucketPutLogging');
        return callback(parsed.error.arsenalError);
    }

    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: request.apiMethods || 'bucketPutLogging',
        request,
    };

    return waterfall([
        next => standardMetadataValidateBucket(metadataValParams, request.actionImplicitDenies, log, (err, bucket) => {
            if (err) {
                return next(err);
            }

            return next(null, bucket);
        }),
        (bucket, next) => {
            const loggingEnabled = parsed.res.getLoggingEnabled();
            if (!loggingEnabled) {
                return next(null, bucket);
            }

            return metadata.getBucket(loggingEnabled.TargetBucket, log, err => {
                if (err) {
                    return next(errorInstances.InvalidTargetBucketForLogging.customizeDescription(err.description));
                }

                return next(null, bucket);
            });
        },
        (bucket, next) => checkExpectedBucketOwner(request.headers, bucket, log, err => {
            if (err) {
                return next(err);
            }

            return next(null, bucket);
        }),
        (bucket, next) => {
            bucket.setBucketLoggingStatus(parsed.res);
            return metadata.updateBucket(bucket.getName(), bucket, log, err => {
                if (err) {
                    return next(err);
                }

                return next();
            });
        },
    ], err => {
        if (err) {
            log.trace('error processing request', { error: err, method: 'bucketPutLogging' });
            monitoring.promMetrics('PUT', bucketName, err.code, 'bucketPutLogging');
            return callback(err);
        }

        monitoring.promMetrics('PUT', bucketName, '200', 'bucketPutLogging');
        return callback();
    });
}

module.exports = bucketPutLogging;
