const { errors, s3middleware } = require('arsenal');
const async = require('async');

const { pushMetric } = require('../utapi/utilities');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const { checkExpectedBucketOwner } = require('./apiUtils/authorization/bucketOwner');
const { metadataValidateBucket } = require('../metadata/metadataUtils');
const escapeForXml = s3middleware.escapeForXml;

/**
 * Bucket Get Encryption - Get bucket SSE configuration
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */

function bucketGetEncryption(authInfo, request, log, callback) {
    const bucketName = request.bucketName;

    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: 'bucketGetEncryption',
        request,
    };

    return async.waterfall([
        next => metadataValidateBucket(metadataValParams, log, next),
        (bucket, next) => checkExpectedBucketOwner(request.headers, bucket, log, err => next(err, bucket)),
        (bucket, next) => {
            // If sseInfo is present but the `mandatory` flag is not set
            // then this info was not created using bucketPutEncryption
            // or by using the x-amz-scal-server-side-encryption header at
            // bucket creation and should not be returned
            const sseInfo = bucket.getServerSideEncryption();
            if (sseInfo === null || !sseInfo.mandatory) {
                log.trace('no server side encryption config found', {
                    bucket: bucketName,
                    method: 'bucketGetEncryption',
                });
                return next(errors.ServerSideEncryptionConfigurationNotFoundError);
            }
            return next(null, bucket, sseInfo);
        },
    ],
    (error, bucket, sseInfo) => {
        const corsHeaders = collectCorsHeaders(request.headers.origin, request.method, bucket);
        if (error) {
            return callback(error, corsHeaders);
        }

        const xml = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<ServerSideEncryptionConfiguration>',
            '<Rule>',
            '<ApplyServerSideEncryptionByDefault>',
            `<SSEAlgorithm>${escapeForXml(sseInfo.algorithm)}</SSEAlgorithm>`,
        ];

        if (sseInfo.configuredMasterKeyId) {
            xml.push(`<KMSMasterKeyID>${escapeForXml(sseInfo.configuredMasterKeyId)}</KMSMasterKeyID>`);
        }

        xml.push(
            '</ApplyServerSideEncryptionByDefault>',
            '<BucketKeyEnabled>false</BucketKeyEnabled>',
            '</Rule>',
            '</ServerSideEncryptionConfiguration>'
        );

        pushMetric('getBucketEncryption', log, {
            authInfo,
            bucket: bucketName,
        });

        return callback(null, xml.join(''), corsHeaders);
    });
}

module.exports = bucketGetEncryption;
