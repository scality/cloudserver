const async = require('async');

const metadata = require('../metadata/wrapper');
const { metadataValidateBucket } = require('../metadata/metadataUtils');
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
        requestType: 'bucketDeleteEncryption',
        request,
    };

    return async.waterfall([
        next => metadataValidateBucket(metadataValParams, log, next),
        (bucket, next) => checkExpectedBucketOwner(request.headers, bucket, log, err => next(err, bucket)),
        (bucket, next) => {
            const sseConfig = bucket.getServerSideEncryption();
            if (sseConfig === null) {
                return next(null, bucket);
            }

            const updatedConfig = {
                mandatory: false,
                algorithm: sseConfig.algorithm,
                cryptoScheme: sseConfig.cryptoScheme,
                masterKeyId: sseConfig.masterKeyId,
            };

            bucket.setServerSideEncryption(updatedConfig);
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
