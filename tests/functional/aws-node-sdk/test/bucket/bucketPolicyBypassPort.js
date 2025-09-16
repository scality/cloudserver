const assert = require('assert');
const { S3, IAM, STS } = require('aws-sdk');
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
    const iamClient = new IAM(iamConfig);

    let policyAllowAllActions;

    before(async () => {
        await bucketUtilAccount.createOne(bucketName);
        await s3ClientAccount
            .putObject({
                Bucket: bucketName,
                Key: objectKey,
                Body: objectContent,
            })
            .promise();

        await s3ClientAccount
            .putBucketPolicy({
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
            })
            .promise();

        // create iam policy allow all actions for user and role
        const policyRes = await iamClient
            .createPolicy({
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
            })
            .promise();

        policyAllowAllActions = policyRes.Policy;
        await iamClient.createUser({ UserName: userName }).promise();
        await iamClient
            .attachUserPolicy({
                UserName: userName,
                PolicyArn: policyAllowAllActions.Arn,
            })
            .promise();
    });

    after(async () => {
        // Remove bucket policy first even if root account can cleanup
        await s3ClientAccount.deleteBucketPolicy({ Bucket: bucketName }).promise();
        await bucketUtilAccount.empty(bucketName);
        await bucketUtilAccount.deleteOne(bucketName);

        if (policyAllowAllActions) {
            await iamClient
                .detachUserPolicy({
                    UserName: userName,
                    PolicyArn: policyAllowAllActions.Arn,
                })
                .promise();
            await iamClient.deletePolicy({ PolicyArn: policyAllowAllActions.Arn }).promise();
        }
        await iamClient.deleteUser({ UserName: userName }).promise();
    });

    it('should allow account root access on s3 port', async () => {
        const getResponse = await s3ClientAccount
            .getObject({
                Bucket: bucketName,
                Key: objectKey,
            })
            .promise();
        assert(getResponse.Body, 'Should be able to get object');
        assert.strictEqual(getResponse.Body.toString(), objectContent);
    });

    describe('IAM User Access Tests', () => {
        let userS3Client;
        let userInternalBypassBPS3Client;

        before(async () => {
            const accessKeyResponse = await iamClient.createAccessKey({ UserName: userName }).promise();
            const { AccessKeyId, SecretAccessKey } = accessKeyResponse.AccessKey;

            // Create S3 client for test user (regular port)
            const userConfig = getConfig('default', {
                credentials: {
                    accessKeyId: AccessKeyId,
                    secretAccessKey: SecretAccessKey,
                },
            });
            userS3Client = new S3(userConfig);

            // Create S3 client for internal port - bypasses bucket policy
            const userInternalBypassBPConfig = getConfig('default', {
                credentials: {
                    accessKeyId: AccessKeyId,
                    secretAccessKey: SecretAccessKey,
                },
            });
            userInternalBypassBPConfig.endpoint = `http://localhost:${internalPortBypassBP}`;
            userInternalBypassBPS3Client = new S3(userInternalBypassBPConfig);
        });

        it('should deny user access on s3 port', async () => {
            try {
                await userS3Client
                    .getObject({
                        Bucket: bucketName,
                        Key: objectKey,
                    })
                    .promise();
                assert.fail('Expected AccessDenied error for getObject');
            } catch (err) {
                assert.strictEqual(err.code, 'AccessDenied');
            }
        });

        it('should bypass user bucket policy on internal port', async () => {
            const getResponse = await userInternalBypassBPS3Client
                .getObject({
                    Bucket: bucketName,
                    Key: objectKey,
                })
                .promise();
            assert(getResponse.Body, 'Should be able to get object on internal port');
            assert.strictEqual(getResponse.Body.toString(), objectContent);
        });
    });

    describe('Role Access Tests', () => {
        let roleS3Client;
        let roleInternalBypassBPS3Client;
        let stsClient;

        before(async () => {
            const roleRes = await iamClient
                .createRole({
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
                })
                .promise();

            await iamClient
                .attachRolePolicy({
                    RoleName: roleName,
                    PolicyArn: policyAllowAllActions.Arn,
                })
                .promise();

            const { AccessKey } = await iamClient.createAccessKey({ UserName: userName }).promise();

            const stsConfig = getConfig('default', {
                region: 'us-east-1',
                credentials: {
                    accessKeyId: AccessKey.AccessKeyId,
                    secretAccessKey: AccessKey.SecretAccessKey,
                },
            });
            stsConfig.endpoint = `http://${vaultHost}:8650`;
            stsClient = new STS(stsConfig);

            // Assume role to get temporary credentials
            const { Credentials } = await stsClient
                .assumeRole({
                    RoleArn: roleRes.Role.Arn,
                    RoleSessionName: 'bp-bypass-session',
                })
                .promise();

            // Create S3 client for role (regular port)
            const roleConfig = getConfig('default', {
                credentials: {
                    accessKeyId: Credentials.AccessKeyId,
                    secretAccessKey: Credentials.SecretAccessKey,
                    sessionToken: Credentials.SessionToken,
                },
            });
            roleS3Client = new S3(roleConfig);

            // Create S3 client for internal port - bypasses bucket policy
            const roleInternalBypassBPConfig = getConfig('default', {
                credentials: {
                    accessKeyId: Credentials.AccessKeyId,
                    secretAccessKey: Credentials.SecretAccessKey,
                    sessionToken: Credentials.SessionToken,
                },
            });
            roleInternalBypassBPConfig.endpoint = `http://localhost:${internalPortBypassBP}`;
            roleInternalBypassBPS3Client = new S3(roleInternalBypassBPConfig);
        });

        after(async () => {
            await iamClient
                .detachRolePolicy({
                    RoleName: roleName,
                    PolicyArn: policyAllowAllActions.Arn,
                })
                .promise();
            await iamClient.deleteRole({ RoleName: roleName }).promise();
        });

        it('should deny role access on s3 port', async () => {
            try {
                await roleS3Client
                    .getObject({
                        Bucket: bucketName,
                        Key: objectKey,
                    })
                    .promise();
                assert.fail('Expected AccessDenied error for getObject');
            } catch (err) {
                assert.strictEqual(err.code, 'AccessDenied');
            }
        });

        it('should bypass role bucket policy on internal port', async () => {
            const getResponse = await roleInternalBypassBPS3Client
                .getObject({
                    Bucket: bucketName,
                    Key: objectKey,
                })
                .promise();
            assert(getResponse.Body, 'Should be able to get object on internal port');
            assert.strictEqual(getResponse.Body.toString(), objectContent);
        });
    });
});
