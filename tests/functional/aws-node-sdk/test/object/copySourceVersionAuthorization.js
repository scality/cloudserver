const assert = require('assert');
const {
    PutObjectCommand,
    GetObjectCommand,
    CopyObjectCommand,
    CreateMultipartUploadCommand,
    UploadPartCopyCommand,
    AbortMultipartUploadCommand,
    PutBucketPolicyCommand,
    DeleteBucketPolicyCommand,
    PutBucketVersioningCommand,
} = require('@aws-sdk/client-s3');
const { v4: uuid } = require('uuid');

const BucketUtility = require('../../lib/utility/bucket-util');
const { makeIamClient, createUserWithPolicy } = require('../../lib/utility/iam-util');
const { config } = require('../../../../../lib/Config');

// this test needs a real vault to create IAM users
const isVaultScality = config.backends.auth !== 'mem';

const describeWithVault = isVaultScality ? describe : describe.skip;

describeWithVault('Copy of a specific source version', () => {
    const secretKey = 'secret';

    async function assertAccessDenied(promise) {
        await assert.rejects(promise, { name: 'AccessDenied' });
    }

    describe('in the same account, without IAM permission to read the source', () => {
        const ownBucketName = `copy-source-own-${uuid()}`;
        const bucketUtil = new BucketUtility('default');
        const iamClient = makeIamClient('default');
        const mpuKey = 'mine/mpu-copy';
        let user;
        let uploadId;

        before(async () => {
            await bucketUtil.createOne(ownBucketName);
            await bucketUtil.s3.send(
                new PutObjectCommand({
                    Bucket: ownBucketName,
                    Key: secretKey,
                    Body: 'secret content',
                }),
            );
            user = await createUserWithPolicy(iamClient, [
                {
                    Effect: 'Allow',
                    Action: ['s3:GetObject', 's3:PutObject'],
                    Resource: `arn:aws:s3:::${ownBucketName}/mine/*`,
                },
            ]);
            const mpuResponse = await user.s3Client.send(
                new CreateMultipartUploadCommand({
                    Bucket: ownBucketName,
                    Key: mpuKey,
                }),
            );
            uploadId = mpuResponse.UploadId;
        });

        after(async () => {
            if (uploadId) {
                await bucketUtil.s3.send(
                    new AbortMultipartUploadCommand({
                        Bucket: ownBucketName,
                        Key: mpuKey,
                        UploadId: uploadId,
                    }),
                );
            }
            await bucketUtil.empty(ownBucketName);
            await bucketUtil.deleteOne(ownBucketName);
            if (user) {
                await user.deleteUser();
            }
        });

        it('should deny CopyObject from the latest version', async () => {
            await assertAccessDenied(
                user.s3Client.send(
                    new CopyObjectCommand({
                        Bucket: ownBucketName,
                        Key: 'mine/copy',
                        CopySource: `${ownBucketName}/${secretKey}`,
                    }),
                ),
            );
        });

        it('should deny CopyObject from the null version', async () => {
            await assertAccessDenied(
                user.s3Client.send(
                    new CopyObjectCommand({
                        Bucket: ownBucketName,
                        Key: 'mine/copy',
                        CopySource: `${ownBucketName}/${secretKey}?versionId=null`,
                    }),
                ),
            );
        });

        it('should deny UploadPartCopy from the null version', async () => {
            await assertAccessDenied(
                user.s3Client.send(
                    new UploadPartCopyCommand({
                        Bucket: ownBucketName,
                        Key: mpuKey,
                        UploadId: uploadId,
                        PartNumber: 1,
                        CopySource: `${ownBucketName}/${secretKey}?versionId=null`,
                    }),
                ),
            );
        });
    });

    describe('with a bucket policy denying s3:GetObjectVersion', () => {
        const policyBucketName = `copy-source-policy-${uuid()}`;
        const bucketUtil = new BucketUtility('default');
        const iamClient = makeIamClient('default');
        const latestContent = 'latest content';
        let user;
        let oldVersionId;

        before(async () => {
            await bucketUtil.createOne(policyBucketName);
            await bucketUtil.s3.send(
                new PutBucketVersioningCommand({
                    Bucket: policyBucketName,
                    VersioningConfiguration: { Status: 'Enabled' },
                }),
            );
            const oldPut = await bucketUtil.s3.send(
                new PutObjectCommand({
                    Bucket: policyBucketName,
                    Key: secretKey,
                    Body: 'old content',
                }),
            );
            oldVersionId = oldPut.VersionId;
            await bucketUtil.s3.send(
                new PutObjectCommand({
                    Bucket: policyBucketName,
                    Key: secretKey,
                    Body: latestContent,
                }),
            );
            await bucketUtil.s3.send(
                new PutBucketPolicyCommand({
                    Bucket: policyBucketName,
                    Policy: JSON.stringify({
                        Version: '2012-10-17',
                        Statement: [
                            {
                                Effect: 'Deny',
                                Principal: '*',
                                Action: 's3:GetObjectVersion',
                                Resource: `arn:aws:s3:::${policyBucketName}/*`,
                            },
                        ],
                    }),
                }),
            );
            user = await createUserWithPolicy(iamClient, [
                {
                    Effect: 'Allow',
                    Action: ['s3:GetObject', 's3:GetObjectVersion', 's3:PutObject'],
                    Resource: `arn:aws:s3:::${policyBucketName}/*`,
                },
            ]);
        });

        after(async () => {
            await bucketUtil.s3.send(new DeleteBucketPolicyCommand({ Bucket: policyBucketName }));
            await bucketUtil.empty(policyBucketName);
            await bucketUtil.deleteOne(policyBucketName);
            if (user) {
                await user.deleteUser();
            }
        });

        it('should deny GetObject of an older version', async () => {
            await assertAccessDenied(
                user.s3Client.send(
                    new GetObjectCommand({
                        Bucket: policyBucketName,
                        Key: secretKey,
                        VersionId: oldVersionId,
                    }),
                ),
            );
        });

        it('should deny CopyObject from an older version', async () => {
            await assertAccessDenied(
                user.s3Client.send(
                    new CopyObjectCommand({
                        Bucket: policyBucketName,
                        Key: 'leak',
                        CopySource: `${policyBucketName}/${secretKey}?versionId=${oldVersionId}`,
                    }),
                ),
            );
        });

        it('should allow CopyObject from the latest version', async () => {
            await user.s3Client.send(
                new CopyObjectCommand({
                    Bucket: policyBucketName,
                    Key: 'latest-copy',
                    CopySource: `${policyBucketName}/${secretKey}`,
                }),
            );
            const getResponse = await user.s3Client.send(
                new GetObjectCommand({
                    Bucket: policyBucketName,
                    Key: 'latest-copy',
                }),
            );
            assert.strictEqual(await getResponse.Body.transformToString(), latestContent);
        });
    });
});
