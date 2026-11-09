const assert = require('assert');
const { Credentials, IAM, S3 } = require('aws-sdk');
const { v4: uuid } = require('uuid');

const BucketUtility = require('../../lib/utility/bucket-util');
const getConfig = require('../support/config');
const { config } = require('../../../../../lib/Config');

// this test needs a real vault to create IAM users
const isVaultScality = config.backends.auth !== 'mem';
const vaultHost = config.vaultd?.host || 'localhost';

const describeWithVault = isVaultScality ? describe : describe.skip;

describeWithVault('DeleteObject of a GOVERNANCE-locked version by an IAM user', () => {
    const bucketName = `delete-governance-bypass-${uuid()}`;
    const bucketUtil = new BucketUtility('default');
    const iamConfig = getConfig('default', { region: 'us-east-1' });
    iamConfig.endpoint = `http://${vaultHost}:8600`; // define outside of getConfig for Integration
    const iamClient = new IAM(iamConfig);
    const users = [];

    const allowDelete = {
        Effect: 'Allow',
        Action: ['s3:DeleteObject'],
        Resource: `arn:aws:s3:::${bucketName}/*`,
    };
    const allowDeleteVersion = {
        Effect: 'Allow',
        Action: ['s3:DeleteObjectVersion'],
        Resource: `arn:aws:s3:::${bucketName}/*`,
    };
    const allowBypass = {
        Effect: 'Allow',
        Action: ['s3:BypassGovernanceRetention'],
        Resource: `arn:aws:s3:::${bucketName}/*`,
    };
    // The bucket policy validation refuses s3:BypassGovernanceRetention on an object resource when it is the only
    // statement, so the bucket policies name the action with a wildcard
    const allowBypassInBucketPolicy = { ...allowBypass, Principal: '*', Action: 's3:Bypass*' };

    async function putVersion(key) {
        const { VersionId } = await bucketUtil.s3
            .putObject({
                Bucket: bucketName,
                Key: key,
                Body: 'content',
            })
            .promise();
        return VersionId;
    }

    async function putLockedVersion(key) {
        const versionId = await putVersion(key);
        await bucketUtil.s3
            .putObjectRetention({
                Bucket: bucketName,
                Key: key,
                VersionId: versionId,
                Retention: {
                    Mode: 'GOVERNANCE',
                    RetainUntilDate: new Date(Date.now() + 24 * 3600 * 1000),
                },
            })
            .promise();
        return versionId;
    }

    // Creates an IAM user with one managed policy and returns its S3 client
    async function createUser(statements) {
        const name = `test-user-${uuid()}`;
        const { Policy } = await iamClient
            .createPolicy({
                PolicyName: name,
                PolicyDocument: JSON.stringify({ Version: '2012-10-17', Statement: statements }),
            })
            .promise();
        await iamClient.createUser({ UserName: name }).promise();
        await iamClient.attachUserPolicy({ UserName: name, PolicyArn: Policy.Arn }).promise();
        const { AccessKey } = await iamClient.createAccessKey({ UserName: name }).promise();
        users.push({ name, policyArn: Policy.Arn, accessKeyId: AccessKey.AccessKeyId });
        return new S3(
            getConfig('default', {
                credentials: new Credentials(AccessKey.AccessKeyId, AccessKey.SecretAccessKey),
            }),
        );
    }

    async function deleteUser({ name, policyArn, accessKeyId }) {
        await iamClient.deleteAccessKey({ UserName: name, AccessKeyId: accessKeyId }).promise();
        await iamClient.detachUserPolicy({ UserName: name, PolicyArn: policyArn }).promise();
        await iamClient.deletePolicy({ PolicyArn: policyArn }).promise();
        await iamClient.deleteUser({ UserName: name }).promise();
    }

    function putBucketPolicy(statement) {
        return bucketUtil.s3
            .putBucketPolicy({
                Bucket: bucketName,
                Policy: JSON.stringify({ Version: '2012-10-17', Statement: [statement] }),
            })
            .promise();
    }

    function deleteBucketPolicy() {
        return bucketUtil.s3.deleteBucketPolicy({ Bucket: bucketName }).promise();
    }

    function deleteWithBypass(s3Client, key, versionId) {
        return s3Client
            .deleteObject({
                Bucket: bucketName,
                Key: key,
                VersionId: versionId,
                BypassGovernanceRetention: true,
            })
            .promise();
    }

    function headVersion(key, versionId) {
        return bucketUtil.s3
            .headObject({
                Bucket: bucketName,
                Key: key,
                VersionId: versionId,
            })
            .promise();
    }

    before(async () => {
        await bucketUtil.s3
            .createBucket({
                Bucket: bucketName,
                ObjectLockEnabledForBucket: true,
            })
            .promise();
    });

    after(async () => {
        await bucketUtil.empty(bucketName, true);
        await bucketUtil.deleteOne(bucketName);
        await Promise.all(users.map(deleteUser));
    });

    it('should deny the bypass header to a user without s3:BypassGovernanceRetention', async () => {
        const key = 'no-bypass-permission';
        const versionId = await putLockedVersion(key);
        const s3Client = await createUser([allowDeleteVersion]);

        await assert.rejects(deleteWithBypass(s3Client, key, versionId), { code: 'AccessDenied' });
        await headVersion(key, versionId);
    });

    it('should deny the bypass header on an unlocked version without s3:BypassGovernanceRetention', async () => {
        const key = 'unlocked-no-bypass-permission';
        const versionId = await putVersion(key);
        const s3Client = await createUser([allowDeleteVersion]);

        await assert.rejects(deleteWithBypass(s3Client, key, versionId), { code: 'AccessDenied' });
        await headVersion(key, versionId);
    });

    it('should add a delete marker with the bypass header and no version ID', async () => {
        const key = 'no-version-id';
        const versionId = await putLockedVersion(key);
        const s3Client = await createUser([allowDelete]);

        const { DeleteMarker } = await deleteWithBypass(s3Client, key);
        assert.strictEqual(DeleteMarker, true);
        await headVersion(key, versionId);
    });

    it('should deny the bypass header when an IAM policy denies s3:BypassGovernanceRetention', async () => {
        const key = 'iam-deny-bypass';
        const versionId = await putLockedVersion(key);
        const s3Client = await createUser([allowDeleteVersion, { ...allowBypass, Effect: 'Deny' }]);

        await assert.rejects(deleteWithBypass(s3Client, key, versionId), { code: 'AccessDenied' });
        await headVersion(key, versionId);
    });

    it('should deny the bypass header when the bucket policy denies s3:BypassGovernanceRetention', async () => {
        const key = 'bucket-policy-deny-bypass';
        const versionId = await putLockedVersion(key);
        const s3Client = await createUser([allowDeleteVersion, allowBypass]);
        await putBucketPolicy({ ...allowBypassInBucketPolicy, Effect: 'Deny' });

        try {
            await assert.rejects(deleteWithBypass(s3Client, key, versionId), { code: 'AccessDenied' });
        } finally {
            await deleteBucketPolicy();
        }
        await headVersion(key, versionId);
    });

    it('should delete the version for a user with s3:BypassGovernanceRetention', async () => {
        const key = 'bypass-permission';
        const versionId = await putLockedVersion(key);
        const s3Client = await createUser([allowDeleteVersion, allowBypass]);

        await deleteWithBypass(s3Client, key, versionId);
        await assert.rejects(headVersion(key, versionId), { statusCode: 404 });
    });

    it('should delete the version for a user allowed s3:BypassGovernanceRetention by the bucket policy', async () => {
        const key = 'bucket-policy-allow-bypass';
        const versionId = await putLockedVersion(key);
        const s3Client = await createUser([allowDeleteVersion]);
        await putBucketPolicy(allowBypassInBucketPolicy);

        try {
            await deleteWithBypass(s3Client, key, versionId);
        } finally {
            await deleteBucketPolicy();
        }
        await assert.rejects(headVersion(key, versionId), { statusCode: 404 });
    });
});
