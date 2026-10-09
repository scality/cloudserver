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
