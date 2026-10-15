const assert = require('assert');
const {
    S3Client,
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
const {
    IAMClient,
    CreateUserCommand,
    CreateAccessKeyCommand,
    DeleteAccessKeyCommand,
    DeleteUserCommand,
} = require('@aws-sdk/client-iam');
const { v4: uuid } = require('uuid');

const getConfig = require('../support/config');
const BucketUtility = require('../../lib/utility/bucket-util');
const { makeIamClient, createUserWithPolicy } = require('../../lib/utility/iam-util');
const { config } = require('../../../../../lib/Config');

const bucketName = `copy-source-bp-${uuid()}`;
const userName = `copy-source-bp-user-${uuid()}`;
const forbiddenKey = 'forbidden/myfile';
const allowedKey = 'allowed/myfile';
const mpuKey = 'allowed/mpu-copy';
const allowedContent = 'allowed content';

// this test needs a real vault to create an IAM user
const isVaultScality = config.backends.auth !== 'mem';
const vaultHost = config.vaultd?.host || 'localhost';

const describeWithVault = isVaultScality ? describe : describe.skip;

describeWithVault('Copy source with a bucket policy scoped to key prefixes', () => {
    const bucketUtilAccount = new BucketUtility('default');
    const s3ClientAccount = bucketUtilAccount.s3;

    const iamConfig = getConfig('default', { region: 'us-east-1' });
    iamConfig.endpoint = `http://${vaultHost}:8600`; // define outside of getConfig for Integration
    const iamClient = new IAMClient(iamConfig);

    let userS3Client;
    let accessKeyId;
    let uploadId;

    async function assertAccessDenied(promise) {
        await assert.rejects(promise, { name: 'AccessDenied' });
    }

    before(async () => {
        await bucketUtilAccount.createOne(bucketName);
        await s3ClientAccount.send(
            new PutObjectCommand({
                Bucket: bucketName,
                Key: forbiddenKey,
                Body: 'forbidden content',
            }),
        );
        await s3ClientAccount.send(
            new PutObjectCommand({
                Bucket: bucketName,
                Key: allowedKey,
                Body: allowedContent,
            }),
        );
        await s3ClientAccount.send(
            new PutBucketPolicyCommand({
                Bucket: bucketName,
                Policy: JSON.stringify({
                    Version: '2012-10-17',
                    Statement: [
                        {
                            Sid: 'AllowReadWriteOnAllowedPrefix',
                            Effect: 'Allow',
                            Principal: '*',
                            Action: ['s3:GetObject', 's3:PutObject'],
                            Resource: `arn:aws:s3:::${bucketName}/allowed/*`,
                        },
                        {
                            Sid: 'DenyReadOnForbiddenPrefix',
                            Effect: 'Deny',
                            Principal: '*',
                            Action: 's3:GetObject',
                            Resource: `arn:aws:s3:::${bucketName}/forbidden/*`,
                        },
                    ],
                }),
            }),
        );

        // IAM user without any identity policy
        await iamClient.send(new CreateUserCommand({ UserName: userName }));
        const accessKeyResponse = await iamClient.send(new CreateAccessKeyCommand({ UserName: userName }));
        accessKeyId = accessKeyResponse.AccessKey.AccessKeyId;
        userS3Client = new S3Client(
            getConfig('default', {
                credentials: {
                    accessKeyId,
                    secretAccessKey: accessKeyResponse.AccessKey.SecretAccessKey,
                },
            }),
        );

        const mpuResponse = await userS3Client.send(
            new CreateMultipartUploadCommand({
                Bucket: bucketName,
                Key: mpuKey,
            }),
        );
        uploadId = mpuResponse.UploadId;
    });

    after(async () => {
        if (uploadId) {
            await s3ClientAccount.send(
                new AbortMultipartUploadCommand({
                    Bucket: bucketName,
                    Key: mpuKey,
                    UploadId: uploadId,
                }),
            );
        }
        await s3ClientAccount.send(new DeleteBucketPolicyCommand({ Bucket: bucketName }));
        await bucketUtilAccount.empty(bucketName);
        await bucketUtilAccount.deleteOne(bucketName);
        if (accessKeyId) {
            await iamClient.send(new DeleteAccessKeyCommand({ UserName: userName, AccessKeyId: accessKeyId }));
        }
        await iamClient.send(new DeleteUserCommand({ UserName: userName }));
    });

    it('should deny GetObject on a source denied by the bucket policy', async () => {
        await assertAccessDenied(
            userS3Client.send(
                new GetObjectCommand({
                    Bucket: bucketName,
                    Key: forbiddenKey,
                }),
            ),
        );
    });

    it('should deny CopyObject from a source denied by the bucket policy', async () => {
        await assertAccessDenied(
            userS3Client.send(
                new CopyObjectCommand({
                    Bucket: bucketName,
                    Key: 'allowed/myfile-copy',
                    CopySource: `${bucketName}/${forbiddenKey}`,
                }),
            ),
        );
    });

    it('should deny UploadPartCopy from a source denied by the bucket policy', async () => {
        await assertAccessDenied(
            userS3Client.send(
                new UploadPartCopyCommand({
                    Bucket: bucketName,
                    Key: mpuKey,
                    UploadId: uploadId,
                    PartNumber: 1,
                    CopySource: `${bucketName}/${forbiddenKey}`,
                }),
            ),
        );
    });

    it('should allow CopyObject from a source allowed by the bucket policy', async () => {
        await userS3Client.send(
            new CopyObjectCommand({
                Bucket: bucketName,
                Key: 'allowed/allowed-copy',
                CopySource: `${bucketName}/${allowedKey}`,
            }),
        );
        const getResponse = await userS3Client.send(
            new GetObjectCommand({
                Bucket: bucketName,
                Key: 'allowed/allowed-copy',
            }),
        );
        assert.strictEqual(await getResponse.Body.transformToString(), allowedContent);
    });

    it('should allow UploadPartCopy from a source allowed by the bucket policy', async () => {
        const res = await userS3Client.send(
            new UploadPartCopyCommand({
                Bucket: bucketName,
                Key: mpuKey,
                UploadId: uploadId,
                PartNumber: 2,
                CopySource: `${bucketName}/${allowedKey}`,
            }),
        );
        assert(res.CopyPartResult.ETag, 'Expected a part ETag');
    });
});

describeWithVault('Copy source authorized against the source bucket policy', () => {
    const secretKey = 'secret';

    async function assertAccessDenied(promise) {
        await assert.rejects(promise, { name: 'AccessDenied' });
    }

    // A copy of a specific version is authorized like a GetObject of that
    // version: a Deny on s3:GetObject alone does not cover it
    describe('with a bucket policy denying s3:GetObject only', () => {
        const getDenyBucketName = `copy-source-get-deny-${uuid()}`;
        const bucketUtil = new BucketUtility('default');
        const iamClient = makeIamClient('default');
        const versionContent = 'version content';
        let user;
        let versionId;

        before(async () => {
            await bucketUtil.createOne(getDenyBucketName);
            await bucketUtil.s3.send(
                new PutBucketVersioningCommand({
                    Bucket: getDenyBucketName,
                    VersioningConfiguration: { Status: 'Enabled' },
                }),
            );
            const putResponse = await bucketUtil.s3.send(
                new PutObjectCommand({
                    Bucket: getDenyBucketName,
                    Key: secretKey,
                    Body: versionContent,
                }),
            );
            versionId = putResponse.VersionId;
            await bucketUtil.s3.send(
                new PutBucketPolicyCommand({
                    Bucket: getDenyBucketName,
                    Policy: JSON.stringify({
                        Version: '2012-10-17',
                        Statement: [
                            {
                                Effect: 'Deny',
                                Principal: '*',
                                Action: 's3:GetObject',
                                Resource: `arn:aws:s3:::${getDenyBucketName}/${secretKey}`,
                            },
                        ],
                    }),
                }),
            );
            user = await createUserWithPolicy(iamClient, [
                {
                    Effect: 'Allow',
                    Action: ['s3:GetObject', 's3:GetObjectVersion', 's3:PutObject'],
                    Resource: `arn:aws:s3:::${getDenyBucketName}/*`,
                },
            ]);
        });

        after(async () => {
            await bucketUtil.s3.send(new DeleteBucketPolicyCommand({ Bucket: getDenyBucketName }));
            await bucketUtil.empty(getDenyBucketName);
            await bucketUtil.deleteOne(getDenyBucketName);
            if (user) {
                await user.deleteUser();
            }
        });

        it('should deny CopyObject from the latest version', async () => {
            await assertAccessDenied(
                user.s3Client.send(
                    new CopyObjectCommand({
                        Bucket: getDenyBucketName,
                        Key: 'latest-copy',
                        CopySource: `${getDenyBucketName}/${secretKey}`,
                    }),
                ),
            );
        });

        it('should allow CopyObject from a specific version', async () => {
            await user.s3Client.send(
                new CopyObjectCommand({
                    Bucket: getDenyBucketName,
                    Key: 'version-copy',
                    CopySource: `${getDenyBucketName}/${secretKey}?versionId=${versionId}`,
                }),
            );
            const getResponse = await user.s3Client.send(
                new GetObjectCommand({
                    Bucket: getDenyBucketName,
                    Key: 'version-copy',
                }),
            );
            assert.strictEqual(await getResponse.Body.transformToString(), versionContent);
        });
    });

    describe('from another account, allowed by the source bucket policy only', () => {
        const sharedBucketName = `copy-source-shared-${uuid()}`;
        const destBucketName = `copy-source-dest-${uuid()}`;
        const sourceBucketUtil = new BucketUtility('default');
        const destBucketUtil = new BucketUtility('lisa');
        const iamClient = makeIamClient('lisa');
        let user;

        before(async () => {
            await sourceBucketUtil.createOne(sharedBucketName);
            await sourceBucketUtil.s3.send(
                new PutObjectCommand({
                    Bucket: sharedBucketName,
                    Key: secretKey,
                    Body: 'secret content',
                }),
            );
            await sourceBucketUtil.s3.send(
                new PutBucketPolicyCommand({
                    Bucket: sharedBucketName,
                    Policy: JSON.stringify({
                        Version: '2012-10-17',
                        Statement: [
                            {
                                Effect: 'Allow',
                                Principal: '*',
                                Action: ['s3:GetObject', 's3:GetObjectVersion'],
                                Resource: `arn:aws:s3:::${sharedBucketName}/*`,
                            },
                        ],
                    }),
                }),
            );
            await destBucketUtil.createOne(destBucketName);
            user = await createUserWithPolicy(iamClient, [
                {
                    Effect: 'Allow',
                    Action: 's3:PutObject',
                    Resource: `arn:aws:s3:::${destBucketName}/*`,
                },
            ]);
        });

        after(async () => {
            await sourceBucketUtil.s3.send(new DeleteBucketPolicyCommand({ Bucket: sharedBucketName }));
            await sourceBucketUtil.empty(sharedBucketName);
            await sourceBucketUtil.deleteOne(sharedBucketName);
            await destBucketUtil.empty(destBucketName);
            await destBucketUtil.deleteOne(destBucketName);
            if (user) {
                await user.deleteUser();
            }
        });

        it('should deny GetObject', async () => {
            await assertAccessDenied(
                user.s3Client.send(
                    new GetObjectCommand({
                        Bucket: sharedBucketName,
                        Key: secretKey,
                    }),
                ),
            );
        });

        it('should deny CopyObject from the latest version', async () => {
            await assertAccessDenied(
                user.s3Client.send(
                    new CopyObjectCommand({
                        Bucket: destBucketName,
                        Key: 'copy',
                        CopySource: `${sharedBucketName}/${secretKey}`,
                    }),
                ),
            );
        });

        it('should deny CopyObject from the null version', async () => {
            await assertAccessDenied(
                user.s3Client.send(
                    new CopyObjectCommand({
                        Bucket: destBucketName,
                        Key: 'copy',
                        CopySource: `${sharedBucketName}/${secretKey}?versionId=null`,
                    }),
                ),
            );
        });
    });
});
