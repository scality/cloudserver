const assert = require('assert');
const {
    CreateBucketCommand,
    PutObjectCommand,
    PutObjectRetentionCommand,
    DeleteObjectCommand,
    HeadObjectCommand,
    PutBucketPolicyCommand,
    DeleteBucketPolicyCommand,
} = require('@aws-sdk/client-s3');
const { v4: uuid } = require('uuid');

const BucketUtility = require('../../lib/utility/bucket-util');
const { makeIamClient, createUserWithPolicy } = require('../../lib/utility/iam-util');
const { config } = require('../../../../../lib/Config');

// this test needs a real vault to create IAM users
const isVaultScality = config.backends.auth !== 'mem';

const describeWithVault = isVaultScality ? describe : describe.skip;

describeWithVault('DeleteObject of a GOVERNANCE-locked version by an IAM user', () => {
    const bucketName = `delete-governance-bypass-${uuid()}`;
    const bucketUtil = new BucketUtility('default');
    const iamClient = makeIamClient('default');
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
        const { VersionId } = await bucketUtil.s3.send(
            new PutObjectCommand({
                Bucket: bucketName,
                Key: key,
                Body: 'content',
            }),
        );
        return VersionId;
    }

    async function putLockedVersion(key) {
        const versionId = await putVersion(key);
        await bucketUtil.s3.send(
            new PutObjectRetentionCommand({
                Bucket: bucketName,
                Key: key,
                VersionId: versionId,
                Retention: {
                    Mode: 'GOVERNANCE',
                    RetainUntilDate: new Date(Date.now() + 24 * 3600 * 1000),
                },
            }),
        );
        return versionId;
    }

    async function createUser(statements) {
        const user = await createUserWithPolicy(iamClient, statements);
        users.push(user);
        return user.s3Client;
    }

    function putBucketPolicy(statement) {
        return bucketUtil.s3.send(
            new PutBucketPolicyCommand({
                Bucket: bucketName,
                Policy: JSON.stringify({ Version: '2012-10-17', Statement: [statement] }),
            }),
        );
    }

    function deleteBucketPolicy() {
        return bucketUtil.s3.send(new DeleteBucketPolicyCommand({ Bucket: bucketName }));
    }

    function deleteWithBypass(s3Client, key, versionId) {
        return s3Client.send(
            new DeleteObjectCommand({
                Bucket: bucketName,
                Key: key,
                VersionId: versionId,
                BypassGovernanceRetention: true,
            }),
        );
    }

    function headVersion(key, versionId) {
        return bucketUtil.s3.send(
            new HeadObjectCommand({
                Bucket: bucketName,
                Key: key,
                VersionId: versionId,
            }),
        );
    }

    before(async () => {
        await bucketUtil.s3.send(
            new CreateBucketCommand({
                Bucket: bucketName,
                ObjectLockEnabledForBucket: true,
            }),
        );
    });

    after(async () => {
        await bucketUtil.empty(bucketName, true);
        await bucketUtil.deleteOne(bucketName);
        await Promise.all(users.map(user => user.deleteUser()));
    });

    it('should deny the bypass header to a user without s3:BypassGovernanceRetention', async () => {
        const key = 'no-bypass-permission';
        const versionId = await putLockedVersion(key);
        const s3Client = await createUser([allowDeleteVersion]);

        await assert.rejects(deleteWithBypass(s3Client, key, versionId), { name: 'AccessDenied' });
        await headVersion(key, versionId);
    });

    it('should deny the bypass header on an unlocked version without s3:BypassGovernanceRetention', async () => {
        const key = 'unlocked-no-bypass-permission';
        const versionId = await putVersion(key);
        const s3Client = await createUser([allowDeleteVersion]);

        await assert.rejects(deleteWithBypass(s3Client, key, versionId), { name: 'AccessDenied' });
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

        await assert.rejects(deleteWithBypass(s3Client, key, versionId), { name: 'AccessDenied' });
        await headVersion(key, versionId);
    });

    it('should deny the bypass header when the bucket policy denies s3:BypassGovernanceRetention', async () => {
        const key = 'bucket-policy-deny-bypass';
        const versionId = await putLockedVersion(key);
        const s3Client = await createUser([allowDeleteVersion, allowBypass]);
        await putBucketPolicy({ ...allowBypassInBucketPolicy, Effect: 'Deny' });

        try {
            await assert.rejects(deleteWithBypass(s3Client, key, versionId), { name: 'AccessDenied' });
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
        await assert.rejects(headVersion(key, versionId), err => err.$metadata.httpStatusCode === 404);
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
        await assert.rejects(headVersion(key, versionId), err => err.$metadata.httpStatusCode === 404);
    });
});
