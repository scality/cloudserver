const { errors, s3middleware } = require('arsenal');

const bucketShield = require('./apiUtils/bucket/bucketShield');
const { isBucketAuthorized } =
    require('./apiUtils/authorization/permissionChecks');
const metadata = require('../metadata/wrapper');
const { pushMetric } = require('../utapi/utilities');
const escapeForXml = s3middleware.escapeForXml;
const collectCorsHeaders = require('../utilities/collectCorsHeaders');

const requestType = 'bucketGetEncryption';

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
    const canonicalID = authInfo.getCanonicalID();

    return metadata.getBucket(bucketName, log, (err, bucket) => {
        if (err) {
            log.debug('metadata getbucket failed', { error: err });
            return callback(err);
        }
        if (bucketShield(bucket, requestType)) {
            return callback(errors.NoSuchBucket);
        }
        log.trace('found bucket in metadata');

        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);

        if (!isBucketAuthorized(bucket, requestType, canonicalID, authInfo,
        log)) {
            log.debug('access denied for account on bucket', {
                requestType,
                method: 'bucketGetEncryption',
            });
            return callback(errors.AccessDenied, null, corsHeaders);
        }

        const sseInfo = bucket.getServerSideEncryption();

        // If sseInfo is present but the `mandatory` flag is not set
        // then this info was not created using bucketPutEncryption
        // and should not be returned
        if (sseInfo === null || !sseInfo.mandatory) {
            log.trace('no server side encryption config found', {
                bucket: bucketName,
                method: 'bucketGetEncryption',
            });
            return callback(errors.ServerSideEncryptionConfigurationNotFoundError);
        }

        const xml = [];
        xml.push(
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<ServerSideEncryptionConfiguration>',
            '<Rule>',
            '<ApplyServerSideEncryptionByDefault>',
            `<SSEAlgorithm>${escapeForXml(sseInfo.algorithm)}</SSEAlgorithm>`,
        );

        if (sseInfo.algorithm === 'aws:kms') {
            xml.push(`<KMSMasterKeyID>${escapeForXml(sseInfo.masterKeyId)}</KMSMasterKeyID>`);
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
