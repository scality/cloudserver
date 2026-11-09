const assert = require('assert');
const {
    PutObjectCommand,
    GetObjectCommand,
    CopyObjectCommand,
    CreateMultipartUploadCommand,
    UploadPartCopyCommand,
    AbortMultipartUploadCommand,
    PutBucketVersioningCommand,
} = require('@aws-sdk/client-s3');
const { v4: uuid } = require('uuid');

const BucketUtility = require('../../lib/utility/bucket-util');
const { makeIamClient, createUserWithPolicy } = require('../../lib/utility/iam-util');
const { config } = require('../../../../../lib/Config');

// this test needs a real vault to create IAM users
const isVaultScality = config.backends.auth !== 'mem';

const describeWithVault = isVaultScality ? describe : describe.skip;

describeWithVault('Copy source version with an IAM policy on s3:VersionId', () => {
    const versionBucketName = `copy-source-vid-${uuid()}`;
    const sourceKey = 'doc';
    const destKey = 'doc-copy';
    const allowedContent = 'allowed content';
    const bucketUtil = new BucketUtility('default');
    const iamClient = makeIamClient('default');

    let user;
    let uploadId;
    // The version IDs as the client sees them
    let deniedVersionId;
    let allowedVersionId;

    async function assertAccessDenied(promise) {
        await assert.rejects(promise, { name: 'AccessDenied' });
    }

    before(async () => {
        await bucketUtil.createOne(versionBucketName);
        await bucketUtil.s3.send(
            new PutBucketVersioningCommand({
                Bucket: versionBucketName,
                VersioningConfiguration: { Status: 'Enabled' },
            }),
        );
        const deniedPut = await bucketUtil.s3.send(
            new PutObjectCommand({
                Bucket: versionBucketName,
                Key: sourceKey,
                Body: 'denied version',
            }),
        );
        deniedVersionId = deniedPut.VersionId;
        const allowedPut = await bucketUtil.s3.send(
            new PutObjectCommand({
                Bucket: versionBucketName,
                Key: sourceKey,
                Body: allowedContent,
            }),
        );
        allowedVersionId = allowedPut.VersionId;

        // IAM user allowed to read and write, except one version of the source
        const bucketArn = `arn:aws:s3:::${versionBucketName}`;
        user = await createUserWithPolicy(iamClient, [
            {
                Effect: 'Allow',
                Action: ['s3:GetObject', 's3:GetObjectVersion', 's3:PutObject'],
                Resource: `${bucketArn}/*`,
            },
            {
                Effect: 'Deny',
                Action: ['s3:GetObject', 's3:GetObjectVersion'],
                Resource: `${bucketArn}/*`,
                Condition: {
                    StringEquals: { 's3:VersionId': deniedVersionId },
                },
            },
        ]);

        const mpuResponse = await user.s3Client.send(
            new CreateMultipartUploadCommand({
                Bucket: versionBucketName,
                Key: destKey,
            }),
        );
        uploadId = mpuResponse.UploadId;
    });

    after(async () => {
        if (uploadId) {
            await bucketUtil.s3.send(
                new AbortMultipartUploadCommand({
                    Bucket: versionBucketName,
                    Key: destKey,
                    UploadId: uploadId,
                }),
            );
        }
        await bucketUtil.empty(versionBucketName);
        await bucketUtil.deleteOne(versionBucketName);
        if (user) {
            await user.deleteUser();
        }
    });

    it('should deny GetObject on the version denied by the IAM policy', async () => {
        await assertAccessDenied(
            user.s3Client.send(
                new GetObjectCommand({
                    Bucket: versionBucketName,
                    Key: sourceKey,
                    VersionId: deniedVersionId,
                }),
            ),
        );
    });

    it('should deny CopyObject from the version denied by the IAM policy', async () => {
        await assertAccessDenied(
            user.s3Client.send(
                new CopyObjectCommand({
                    Bucket: versionBucketName,
                    Key: destKey,
                    CopySource: `${versionBucketName}/${sourceKey}?versionId=${deniedVersionId}`,
                }),
            ),
        );
    });

    it('should deny UploadPartCopy from the version denied by the IAM policy', async () => {
        await assertAccessDenied(
            user.s3Client.send(
                new UploadPartCopyCommand({
                    Bucket: versionBucketName,
                    Key: destKey,
                    UploadId: uploadId,
                    PartNumber: 1,
                    CopySource: `${versionBucketName}/${sourceKey}?versionId=${deniedVersionId}`,
                }),
            ),
        );
    });

    it('should allow CopyObject from a version not denied by the IAM policy', async () => {
        await user.s3Client.send(
            new CopyObjectCommand({
                Bucket: versionBucketName,
                Key: destKey,
                CopySource: `${versionBucketName}/${sourceKey}?versionId=${allowedVersionId}`,
            }),
        );
        const getResponse = await user.s3Client.send(
            new GetObjectCommand({
                Bucket: versionBucketName,
                Key: destKey,
            }),
        );
        assert.strictEqual(await getResponse.Body.transformToString(), allowedContent);
    });
});
