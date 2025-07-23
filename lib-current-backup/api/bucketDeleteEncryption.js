const async = require('async');

const metadata = require('../metadata/wrapper');
const { standardMetadataValidateBucket } = require('../metadata/metadataUtils');
const { pushMetric } = require('../utapi/utilities');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const { checkExpectedBucketOwner } = require('./apiUtils/authorization/bucketOwner');

/**
 * Bucket Delete Encryption - Delete bucket SSE configuration
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */

function bucketDeleteEncryption(authInfo, request, log, callback) {
    const bucketName = request.bucketName;

    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: request.apiMethods || 'bucketDeleteEncryption',
        request,
    };

    return async.waterfall([
        next => standardMetadataValidateBucket(metadataValParams, request.actionImplicitDenies, log, next),
        (bucket, next) => checkExpectedBucketOwner(request.headers, bucket, log, err => next(err, bucket)),
        (bucket, next) => {
            const sseConfig = bucket.getServerSideEncryption();

            if (sseConfig === null) {
                return next(null, bucket);
            }

            const { isAccountEncryptionEnabled, masterKeyId, algorithm, cryptoScheme } = sseConfig;

            let updatedSseConfig = null;

            if (!isAccountEncryptionEnabled && masterKeyId) {
                // Keep the encryption configuration as a "cache" to avoid generating a new master key:
                // - if the default encryption master key is defined at the bucket level (!isAccountEncryptionEnabled),
                // - and if a bucket-level default encryption key is already set.
                // This "cache" is implemented by storing the configuration in the bucket metadata
                // with mandatory set to false, making sure it remains hidden for `getBucketEncryption` operations.
                // There is no need to cache the configuration if the default encryption master key is
                // managed at the account level, as the master key id in that case is stored directly in
                // the account metadata.
                updatedSseConfig = {
                    mandatory: false,
                    algorithm,
                    cryptoScheme,
                    masterKeyId,
                };
            }

            bucket.setServerSideEncryption(updatedSseConfig);
            return metadata.updateBucket(bucketName, bucket, log, err => next(err, bucket));
        },
    ],
    (err, bucket) => {
        const corsHeaders = collectCorsHeaders(request.headers.origin, request.method, bucket);
        if (err) {
            log.trace('error processing request', { error: err, method: 'bucketDeleteEncryption' });
            return callback(err, corsHeaders);
        }
        pushMetric('deleteBucketEncryption', log, {
            authInfo,
            bucket: bucketName,
        });
        return callback(null, corsHeaders);
    });
}

module.exports = bucketDeleteEncryption;
