const assert = require('assert');
const {
    S3Client,
    PutObjectCommand,
    GetObjectCommand,
    PutBucketPolicyCommand,
    DeleteBucketPolicyCommand,
} = require('@aws-sdk/client-s3');
const {
    IAMClient,
    CreatePolicyCommand,
    CreateUserCommand,
    AttachUserPolicyCommand,
    DetachUserPolicyCommand,
    DeletePolicyCommand,
    DeleteUserCommand,
    CreateAccessKeyCommand,
    CreateRoleCommand,
    AttachRolePolicyCommand,
    DetachRolePolicyCommand,
    DeleteRoleCommand,
} = require('@aws-sdk/client-iam');
const { STSClient, AssumeRoleCommand } = require('@aws-sdk/client-sts');
const { v4: uuid } = require('uuid');

const getConfig = require('../support/config');
const BucketUtility = require('../../lib/utility/bucket-util');
const { config } = require('../../../../../lib/Config');

const bucketName = `bp-bypass-${uuid()}`;
const userName = `bp-bypass-user-${uuid()}`;
const roleName = `bp-bypass-role-${uuid()}`;
const objectKey = 'test-object';
const objectContent = 'test content';

// this test needs a real vault to create user and role
const isVaultScality = config.backends.auth !== 'mem';
const internalPortBypassBP = config.internalPort;
const vaultHost = config.vaultd?.host || 'localhost';

// will run for jobs s3c-ft-tests and in Integration tests
const describeBypass = isVaultScality && internalPortBypassBP ? describe : describe.skip;
describeBypass('Bucket Policy Bypass Port', () => {
    const bucketUtilAccount = new BucketUtility('default');
    const s3ClientAccount = bucketUtilAccount.s3;

    const iamConfig = getConfig('default', { region: 'us-east-1' });
    iamConfig.endpoint = `http://${vaultHost}:8600`; // define outside of getConfig for Integration
    const iamClient = new IAMClient(iamConfig);

    let policyAllowAllActions;

    before(async () => {
        await bucketUtilAccount.createOne(bucketName);
        await s3ClientAccount.send(
            new PutObjectCommand({
                Bucket: bucketName,
                Key: objectKey,
                Body: objectContent,
            }),
        );

        await s3ClientAccount.send(
            new PutBucketPolicyCommand({
                Bucket: bucketName,
                Policy: JSON.stringify({
                    Version: '2012-10-17',
                    Statement: [
                        {
                            Sid: 'DenyAllAccess',
                            Effect: 'Deny',
                            Principal: '*',
                            Action: 's3:*',
                            Resource: [`arn:aws:s3:::${bucketName}`, `arn:aws:s3:::${bucketName}/*`],
                        },
                    ],
                }),
            }),
        );

        // create iam policy allow all actions for user and role
        const policyRes = await iamClient.send(
            new CreatePolicyCommand({
                PolicyName: 'bp-bypass-policy',
                PolicyDocument: JSON.stringify({
                    Version: '2012-10-17',
                    Statement: [
                        {
                            Sid: 'AllowAllActions',
                            Effect: 'Allow',
                            Action: '*',
                            Resource: ['*'],
                        },
                    ],
                }),
            }),
        );

        policyAllowAllActions = policyRes.Policy;
        await iamClient.send(new CreateUserCommand({ UserName: userName }));
        await iamClient.send(
            new AttachUserPolicyCommand({
                UserName: userName,
                PolicyArn: policyAllowAllActions.Arn,
            }),
        );
    });

    after(async () => {
        // Remove bucket policy first even if root account can cleanup
        await s3ClientAccount.send(new DeleteBucketPolicyCommand({ Bucket: bucketName }));
        await bucketUtilAccount.empty(bucketName);
        await bucketUtilAccount.deleteOne(bucketName);

        if (policyAllowAllActions) {
            await iamClient.send(
                new DetachUserPolicyCommand({
                    UserName: userName,
                    PolicyArn: policyAllowAllActions.Arn,
                }),
            );
            await iamClient.send(new DeletePolicyCommand({ PolicyArn: policyAllowAllActions.Arn }));
        }
        await iamClient.send(new DeleteUserCommand({ UserName: userName }));
    });

    it('should allow account root access on s3 port', async () => {
        const getResponse = await s3ClientAccount.send(
            new GetObjectCommand({
                Bucket: bucketName,
                Key: objectKey,
            }),
        );
        assert(getResponse.Body, 'Should be able to get object');
        const bodyString = await getResponse.Body.transformToString();
        assert.strictEqual(bodyString, objectContent);
    });

    describe('IAM User Access Tests', () => {
        let userS3Client;
        let userInternalBypassBPS3Client;

        before(async () => {
            const accessKeyResponse = await iamClient.send(new CreateAccessKeyCommand({ UserName: userName }));
            const { AccessKeyId, SecretAccessKey } = accessKeyResponse.AccessKey;

            // Create S3 client for test user (regular port)
            const userConfig = getConfig('default', {
                credentials: {
                    accessKeyId: AccessKeyId,
                    secretAccessKey: SecretAccessKey,
                },
            });
            userS3Client = new S3Client(userConfig);

            // Create S3 client for internal port - bypasses bucket policy
            const userInternalBypassBPConfig = getConfig('default', {
                credentials: {
                    accessKeyId: AccessKeyId,
                    secretAccessKey: SecretAccessKey,
                },
            });
            userInternalBypassBPConfig.endpoint = `http://localhost:${internalPortBypassBP}`;
            userInternalBypassBPS3Client = new S3Client(userInternalBypassBPConfig);
        });

        it('should deny user access on s3 port', async () => {
            try {
                await userS3Client.send(
                    new GetObjectCommand({
                        Bucket: bucketName,
                        Key: objectKey,
                    }),
                );
                assert.fail('Expected AccessDenied error for getObject');
            } catch (err) {
                assert.strictEqual(err.name, 'AccessDenied');
            }
        });

        it('should bypass user bucket policy on internal port', async () => {
            const getResponse = await userInternalBypassBPS3Client.send(
                new GetObjectCommand({
                    Bucket: bucketName,
                    Key: objectKey,
                }),
            );
            assert(getResponse.Body, 'Should be able to get object on internal port');
            const bodyString = await getResponse.Body.transformToString();
            assert.strictEqual(bodyString, objectContent);
        });
    });

    describe('Role Access Tests', () => {
        let roleS3Client;
        let roleInternalBypassBPS3Client;
        let stsClient;

        before(async () => {
            const roleRes = await iamClient.send(
                new CreateRoleCommand({
                    RoleName: roleName,
                    AssumeRolePolicyDocument: JSON.stringify({
                        Version: '2012-10-17',
                        Statement: [
                            {
                                Effect: 'Allow',
                                Principal: '*',
                                Action: 'sts:AssumeRole',
                            },
                        ],
                    }),
                }),
            );

            await iamClient.send(
                new AttachRolePolicyCommand({
                    RoleName: roleName,
                    PolicyArn: policyAllowAllActions.Arn,
                }),
            );

            const accessKeyResponse = await iamClient.send(new CreateAccessKeyCommand({ UserName: userName }));

            const stsConfig = getConfig('default', {
                region: 'us-east-1',
                credentials: {
                    accessKeyId: accessKeyResponse.AccessKey.AccessKeyId,
                    secretAccessKey: accessKeyResponse.AccessKey.SecretAccessKey,
                },
            });
            stsConfig.endpoint = `http://${vaultHost}:8650`;
            stsClient = new STSClient(stsConfig);

            // Assume role to get temporary credentials
            const assumeRoleResponse = await stsClient.send(
                new AssumeRoleCommand({
                    RoleArn: roleRes.Role.Arn,
                    RoleSessionName: 'bp-bypass-session',
                }),
            );
            const credentials = assumeRoleResponse.Credentials;

            // Create S3 client for role (regular port)
            const roleConfig = getConfig('default', {
                credentials: {
                    accessKeyId: credentials.AccessKeyId,
                    secretAccessKey: credentials.SecretAccessKey,
                    sessionToken: credentials.SessionToken,
                },
            });
            roleS3Client = new S3Client(roleConfig);

            // Create S3 client for internal port - bypasses bucket policy
            const roleInternalBypassBPConfig = getConfig('default', {
                credentials: {
                    accessKeyId: credentials.AccessKeyId,
                    secretAccessKey: credentials.SecretAccessKey,
                    sessionToken: credentials.SessionToken,
                },
            });
            roleInternalBypassBPConfig.endpoint = `http://localhost:${internalPortBypassBP}`;
            roleInternalBypassBPS3Client = new S3Client(roleInternalBypassBPConfig);
        });

        after(async () => {
            await iamClient.send(
                new DetachRolePolicyCommand({
                    RoleName: roleName,
                    PolicyArn: policyAllowAllActions.Arn,
                }),
            );
            await iamClient.send(new DeleteRoleCommand({ RoleName: roleName }));
        });

        it('should deny role access on s3 port', async () => {
            try {
                await roleS3Client.send(
                    new GetObjectCommand({
                        Bucket: bucketName,
                        Key: objectKey,
                    }),
                );
                assert.fail('Expected AccessDenied error for getObject');
            } catch (err) {
                assert.strictEqual(err.name, 'AccessDenied');
            }
        });

        it('should bypass role bucket policy on internal port', async () => {
            const getResponse = await roleInternalBypassBPS3Client.send(
                new GetObjectCommand({
                    Bucket: bucketName,
                    Key: objectKey,
                }),
            );
            assert(getResponse.Body, 'Should be able to get object on internal port');
            const bodyString = await getResponse.Body.transformToString();
            assert.strictEqual(bodyString, objectContent);
        });
    });
});
