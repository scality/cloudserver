const { errors } = require('arsenal');
const async = require('async');

const bucketShield = require('./apiUtils/bucket/bucketShield');
const { parseEncryptionXml } = require('./apiUtils/bucket/bucketEncryption');
const { isBucketAuthorized } = require('./apiUtils/authorization/permissionChecks');
const metadata = require('../metadata/wrapper');
const kms = require('../kms/wrapper');
const { pushMetric } = require('../utapi/utilities');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');

const requestType = 'bucketPutEncryption';

/**
 * Bucket Put Encryption - Put bucket SSE configuration
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */

function bucketPutEncryption(authInfo, request, log, callback) {
    const bucketName = request.bucketName;
    const canonicalID = authInfo.getCanonicalID();

    return async.waterfall([
        next => metadata.getBucket(bucketName, log, (err, bucket) => {
            if (err) {
                log.debug('metadata getbucket failed', { error: err });
                return next(err);
            }

            if (bucketShield(bucket, requestType)) {
                return next(errors.NoSuchBucket);
            }

            log.trace('found bucket in metadata', {
                bucket: bucketName,
                method: 'bucketPutEncryption',
            });

            if (!isBucketAuthorized(bucket, requestType, canonicalID, authInfo, log)) {
                log.debug('access denied for account on bucket', {
                    requestType,
                    method: 'bucketPutEncryption',
                });
                return next(errors.AccessDenied, null);
            }
            return next(null, bucket);
        }),
        (bucket, next) => {
            log.trace('parsing encryption config', { method: 'bucketPutEncryption' });
            return parseEncryptionXml(request.post, log, (err, encryptionConfig) => {
                if (err) {
                    return next(err);
                }
                return next(null, bucket, encryptionConfig);
            });
        },
        (bucket, encryptionConfig, next) => {
            const existingConfig = bucket.getServerSideEncryption();
            if (existingConfig === null) {
                return kms.bucketLevelEncryption(bucket.getName(), encryptionConfig, log,
                    (err, updatedConfig) => {
                        if (err) {
                            return next(err);
                        }
                        return next(null, bucket, updatedConfig);
                    });
            }

            const updatedConfig = {
                mandatory: true,
                algorithm: encryptionConfig.algorithm,
                cryptoScheme: existingConfig.cryptoScheme,
                masterKeyId: existingConfig.masterKeyId,
            };

            const { configuredMasterKeyId } = encryptionConfig;
            if (configuredMasterKeyId) {
                updatedConfig.configuredMasterKeyId = configuredMasterKeyId;
            }

            return next(null, bucket, updatedConfig);
        },
        (bucket, updatedConfig, next) => {
            bucket.setServerSideEncryption(updatedConfig);
            metadata.updateBucket(bucket.getName(), bucket, log, err => next(err, bucket));
        },
    ],
    (err, bucket) => {
        const corsHeaders = collectCorsHeaders(request.headers.origin, request.method, bucket);
        if (err) {
            log.trace('error processing request', { error: err, method: 'bucketPutEncryption' });
            return callback(err, corsHeaders);
        }
        pushMetric('putBucketEncryption', log, {
            authInfo,
            bucket: bucketName,
        });
        return callback(null, corsHeaders);
    });
}

module.exports = bucketPutEncryption;
