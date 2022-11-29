const assert = require('assert');
const DummyRequest = require('./DummyRequest');
const { authBucketPut } = require('../../lib/api/bucketPut');
const prepareRequestContexts = require('../../lib/api/apiUtils/authorization/prepareRequestContexts.js');

const sourceBucket = 'bucket';
const sourceObject = 'object';
const apiMatrix = [
    {
        name: 'multipartDelete',
        expectedPermissions: ['s3:AbortMultipartUpload'],
    },
    {
        name: 'objectCopy',
        headers: {
            'x-amz-tagging': true,
            'x-amz-tagging-directive': 'REPLACE',
        },
        expectedPermissions: ['s3:GetObject', 's3:PutObject', 's3:PutObjectTagging'],
    },
    {
        name: 'objectCopy',
        headers: {
            'x-amz-tagging': true,
            'x-amz-tagging-directive': 'COPY',
        },
        expectedPermissions: ['s3:GetObject', 's3:PutObject'],
    },
    {
        name: 'initiateMultipartUpload',
        expectedPermissions: ['s3:PutObject'],
    },
    {
        name: 'bucketDelete',
        expectedPermissions: ['s3:DeleteBucket'],
    },
    {
        name: 'bucketDeleteCors',
        expectedPermissions: ['s3:PutBucketCORS'],
    },
    {
        name: 'bucketDeleteEncryption',
        expectedPermissions: ['s3:PutEncryptionConfiguration'],
    },
    {
        name: 'bucketDeleteLifecycle',
        expectedPermissions: ['s3:PutLifecycleConfiguration'],
    },
    {
        name: 'bucketDeletePolicy',
        expectedPermissions: ['s3:DeleteBucketPolicy'],
    },
    {
        name: 'bucketDeleteTagging',
        expectedPermissions: ['s3:PutBucketTagging'],
    },
    {
        name: 'bucketDeleteWebsite',
        expectedPermissions: ['s3:DeleteBucketWebsite'],
    },
    {
        name: 'objectDelete',
        expectedPermissions: ['s3:DeleteObject'],
    },
    {
        name: 'objectDeleteTagging',
        expectedPermissions: ['s3:DeleteObjectTagging'],
    },
    {
        name: 'bucketGetACL',
        expectedPermissions: ['s3:GetBucketAcl'],
    },
    {
        name: 'bucketGetCors',
        expectedPermissions: ['s3:GetBucketCORS'],
    },
    {
        name: 'bucketGetEncryption',
        expectedPermissions: ['s3:GetEncryptionConfiguration'],
    },
    {
        name: 'bucketGetLifecycle',
        expectedPermissions: ['s3:GetLifecycleConfiguration'],
    },
    {
        name: 'bucketGetLocation',
        expectedPermissions: ['s3:GetBucketLocation'],
    },
    {
        name: 'bucketGetNotification',
        expectedPermissions: ['s3:GetBucketNotification'],
    },
    {
        name: 'bucketGetPolicy',
        expectedPermissions: ['s3:GetBucketPolicy'],
    },
    {
        name: 'bucketGetReplication',
        expectedPermissions: ['s3:GetReplicationConfiguration'],
    },
    {
        name: 'bucketGetTagging',
        expectedPermissions: ['s3:GetBucketTagging'],
    },
    {
        name: 'bucketGetVersioning',
        expectedPermissions: ['s3:GetBucketVersioning'],
    },
    {
        name: 'bucketGetWebsite',
        expectedPermissions: ['s3:GetBucketWebsite'],
    },
    {
        name: 'objectGet',
        expectedPermissions: ['s3:GetObject', 's3:GetObjectTagging'],
    },
    {
        name: 'objectGet',
        headers: {
            'x-amz-version-id': '1',
        },
        expectedPermissions: ['s3:GetObjectVersion', 's3:GetObject', 's3:GetObjectTagging'],
    },
    {
        name: 'objectGetACL',
        expectedPermissions: ['s3:GetObjectAcl'],
    },
    {
        name: 'objectGetLegalHold',
        expectedPermissions: ['s3:GetObjectLegalHold'],
    },
    {
        name: 'bucketGetObjectLock',
        expectedPermissions: ['s3:GetBucketObjectLockConfiguration'],
    },
    {
        name: 'objectGetRetention',
        expectedPermissions: ['s3:GetObjectRetention'],
    },
    {
        name: 'objectGetTagging',
        expectedPermissions: ['s3:GetObjectTagging'],
    },
    {
        name: 'objectGetTagging',
        headers: {
            'x-amz-version-id': '1',
        },
        expectedPermissions: ['s3:GetObjectTagging', 's3:GetObjectVersionTagging'],
    },
    {
        name: 'bucketGet',
        expectedPermissions: ['s3:ListBucket'],
    },
    {
        name: 'objectHead',
        expectedPermissions: ['s3:GetObject'],
    },
    {
        name: 'objectHead',
        headers: {
            'x-amz-version-id': '1',
        },
        expectedPermissions: ['s3:GetObject', 's3:GetObjectVersion'],
    },
    {
        name: 'listParts',
        expectedPermissions: ['s3:ListMultipartUploadParts'],
    },
    {
        name: 'listObjectVersions',
        expectedPermissions: ['s3:ListBucketVersions'],
    },
    {
        name: 'listParts',
        expectedPermissions: ['s3:ListMultipartUploadParts'],
    },
    {
        name: 'bucketPutACL',
        expectedPermissions: ['s3:PutBucketAcl'],
    },
    {
        name: 'bucketPutCors',
        expectedPermissions: ['s3:PutBucketCORS'],
    },
    {
        name: 'bucketPutEncryption',
        expectedPermissions: ['s3:PutEncryptionConfiguration'],
    },
    {
        name: 'bucketPutLifecycle',
        expectedPermissions: ['s3:PutLifecycleConfiguration'],
    },
    {
        name: 'bucketPutNotification',
        expectedPermissions: ['s3:PutBucketNotification'],
    },
    {
        name: 'bucketPutPolicy',
        expectedPermissions: ['s3:PutBucketPolicy'],
    },
    {
        name: 'bucketPutReplication',
        expectedPermissions: ['s3:PutReplicationConfiguration'],
    },
    {
        name: 'bucketPutTagging',
        expectedPermissions: ['s3:PutBucketTagging'],
    },
    {
        name: 'bucketPutVersioning',
        expectedPermissions: ['s3:PutBucketVersioning'],
    },
    {
        name: 'bucketPutWebsite',
        expectedPermissions: ['s3:PutBucketWebsite'],
    },
    {
        name: 'objectPut',
        expectedPermissions: ['s3:PutObject'],
    },
    {
        name: 'objectPut',
        headers: {
            'x-amz-object-lock-legal-hold-status': 'ON',
            'x-amz-object-lock-mode': 'GOVERNANCE',
            'x-amz-tagging': 'Key1=Value1',
            'x-amz-acl': 'private',
        },
        expectedPermissions: [
            's3:PutObject',
            's3:PutObjectTagging',
            's3:PutObjectLegalHold',
            's3:PutObjectAcl',
            's3:PutObjectRetention',
        ],
    },
    {
        name: 'objectPut',
        headers: {
            'x-amz-version-id': '1',
        },
        expectedPermissions: [
            's3:PutObject',
            's3:PutObjectVersionTagging',
        ],
    },
    {
        name: 'objectPutACL',
        expectedPermissions: ['s3:PutObjectAcl'],
    },
    {
        name: 'objectPutLegalHold',
        expectedPermissions: ['s3:PutObjectLegalHold'],
    },
    {
        name: 'bucketPutObjectLock',
        expectedPermissions: ['s3:PutBucketObjectLockConfiguration'],
    },
    {
        name: 'objectPutRetention',
        expectedPermissions: ['s3:PutObjectRetention'],
    },
    {
        name: 'objectPutTagging',
        expectedPermissions: ['s3:PutObjectTagging'],
    },
    {
        name: 'objectPutTagging',
        headers: {
            'x-amz-version-id': '1',
        },
        expectedPermissions: ['s3:PutObjectTagging', 's3:PutObjectVersionTagging'],
    },
    {
        name: 'objectPutPart',
        expectedPermissions: ['s3:PutObject'],
    },
    {
        name: 'objectPutCopyPart',
        expectedPermissions: ['s3:GetObject', 's3:PutObject'],
    },
];


function prepareDummyRequest(headers = {}) {
    const request = new DummyRequest({
        hostname: 'localhost',
        port: 80,
        headers,
        socket: {
            remoteAddress: '0.0.0.0',
        },
    });
    return request;
}

describe('Policies: permission checks for S3 APIs', () => {
    apiMatrix.forEach(api => {
        if (api.name.length === 0) return;
        const message = `should return ${api.expectedPermissions.join(', ')} in requestContextParams for ${api.name}` +
            `${(api.headers && api.headers.length) > 0 ?
                ` with headers ${api.headers.map(el => el[0]).join(', ')}` : ''}`;
        it(message, () => {
            const request = prepareDummyRequest(api.headers);
            const requestContexts = prepareRequestContexts(api.name, request,
                sourceBucket, sourceObject);
            const requestedActions = requestContexts.map(rq => rq.getAction());
            assert.deepStrictEqual(requestedActions, api.expectedPermissions);
        });
    });

    describe('CreateBucket', () => {
        function putBucketApiMethods(headers) {
            const request = prepareDummyRequest(headers);
            const result = authBucketPut(null, 'name', null, request, null);
            return result.map(req => req.apiMethod);
        }

        it('should return s3:PutBucket without any provided header', () => {
            assert.deepStrictEqual(
                putBucketApiMethods(),
                ['bucketPut'],
            );
        });

        it('should return s3:CreateBucket, s3:PutBucketVersioning and s3:PutBucketObjectLockConfiguration' +
            ' with object-lock headers', () => {
            assert.deepStrictEqual(
                putBucketApiMethods({ 'x-amz-bucket-object-lock-enabled': 'true' }),
                ['bucketPut', 'bucketPutObjectLock', 'bucketPutVersioning'],
            );
        });

        it('should return s3:CreateBucket and s3:PutBucketAcl' +
            ' with ACL headers', () => {
            assert.deepStrictEqual(
                putBucketApiMethods({ 'x-amz-grant-read': 'private' }),
                ['bucketPut', 'bucketPutACL'],
            );
        });
    });
});
