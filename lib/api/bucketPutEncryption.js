const async = require('async');

const { parseEncryptionXml } = require('./apiUtils/bucket/bucketEncryption');
const { checkExpectedBucketOwner } = require('./apiUtils/authorization/bucketOwner');
const metadata = require('../metadata/wrapper');
const { standardMetadataValidateBucket } = require('../metadata/metadataUtils');
const kms = require('../kms/wrapper');
const { pushMetric } = require('../utapi/utilities');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');

/**
 * Bucket Put Encryption - Put bucket SSE configuration
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */

function bucketPutEncryption(authInfo, request, log, callback) {
    const { bucketName } = request;

    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: request.apiMethods || 'bucketPutEncryption',
        request,
    };

    return async.waterfall([
        next => standardMetadataValidateBucket(metadataValParams, request.actionImplicitDenies, log, next),
        (bucket, next) => checkExpectedBucketOwner(request.headers, bucket, log, err => next(err, bucket)),
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
            // Check if encryption is not configured or if a default master key has not been created yet.
            if (existingConfig === null || !existingConfig.masterKeyId) {
                return kms.bucketLevelEncryption(bucket, encryptionConfig, log,
                    (err, updatedConfig) => {
                        if (err) {
                            return next(err);
                        }
                        return next(null, bucket, updatedConfig);
                    });
            }

            // If encryption is already configured and a default master key exists

            // If the request does not specify a custom key, reuse the existing default master key id
            // This ensures that a new default master key is not generated every time
            // `putBucketEncryption` is called, avoiding unnecessary key creation
            const updatedConfig = {
                mandatory: true,
                algorithm: encryptionConfig.algorithm,
                cryptoScheme: existingConfig.cryptoScheme,
                masterKeyId: existingConfig.masterKeyId,
            };

            // If the request specifies a custom master key id, store it in the updated configuration
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
