const { waterfall } = require('async');
const arsenal = require('arsenal');

const errors = arsenal.errors;
const ObjectLockConfiguration = arsenal.models.ObjectLockConfiguration;

const parseXML = require('../utilities/parseXML');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const metadata = require('../metadata/wrapper');
const { metadataValidateBucket } = require('../metadata/metadataUtils');
const { pushMetric } = require('../utapi/utilities');

/**
 * Bucket Put Object Lock - Create or update bucket object lock configuration
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */

function bucketPutObjectLock(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketPutObjectLock' });

    const bucketName = request.bucketName;
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: 'bucketPutObjectLock',
        request,
    };
    return waterfall([
        next => parseXML(request.post, log, next),
        (parsedXml, next) => {
            const lockConfigClass = new ObjectLockConfiguration(parsedXml);
            // if there was an error getting object lock configuration,
            // returned configObj will contain 'error' key
            process.nextTick(() => {
                const configObj = lockConfigClass.
                    getValidatedObjectLockConfiguration();
                return next(configObj.error || null, configObj);
            });
        },
        (objectLockConfig, next) => metadataValidateBucket(metadataValParams,
            log, (err, bucket) => {
                if (err) {
                    return next(err, bucket);
                }
                return next(null, bucket, objectLockConfig);
            }),
        (bucket, objectLockConfig, next) => {
            const isObjectLockEnabled = bucket.isObjectLockEnabled();
            process.nextTick(() => {
                if (!isObjectLockEnabled) {
                    return next(errors.InvalidBucketState.customizeDescription(
                        'Object Lock configuration cannot be enabled on ' +
                        'existing buckets'), bucket);
                }
                return next(null, bucket, objectLockConfig);
            });
        },
        (bucket, objectLockConfig, next) => {
            bucket.setObjectLockConfiguration(objectLockConfig);
            metadata.updateBucket(bucket.getName(), bucket, log, err =>
                next(err, bucket));
        },
    ], (err, bucket) => {
        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (err) {
            log.trace('error processing request', { error: err,
                method: 'bucketPutObjectLock' });
            return callback(err, corsHeaders);
        }
        pushMetric('putBucketObjectLock', log, {
            authInfo,
            bucket: bucketName,
        });
        return callback(null, corsHeaders);
    });
}

module.exports = bucketPutObjectLock;
