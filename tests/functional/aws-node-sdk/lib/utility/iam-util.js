const { S3Client } = require('@aws-sdk/client-s3');
const {
    IAMClient,
    CreateUserCommand,
    CreateAccessKeyCommand,
    DeleteAccessKeyCommand,
    DeleteUserCommand,
    CreatePolicyCommand,
    AttachUserPolicyCommand,
    DetachUserPolicyCommand,
    DeletePolicyCommand,
} = require('@aws-sdk/client-iam');
const { v4: uuid } = require('uuid');

const getConfig = require('../../test/support/config');
const { config } = require('../../../../../lib/Config');

const vaultHost = config.vaultd?.host || 'localhost';

function makeIamClient(profile) {
    const iamConfig = getConfig(profile, { region: 'us-east-1' });
    iamConfig.endpoint = `http://${vaultHost}:8600`; // define outside of getConfig for Integration
    return new IAMClient(iamConfig);
}

// Creates an IAM user with one managed policy, returns its S3 client and a
// function removing the user
async function createUserWithPolicy(iamClient, statements) {
    const name = `test-user-${uuid()}`;
    const policyResponse = await iamClient.send(
        new CreatePolicyCommand({
            PolicyName: name,
            PolicyDocument: JSON.stringify({ Version: '2012-10-17', Statement: statements }),
        }),
    );
    const policyArn = policyResponse.Policy.Arn;
    await iamClient.send(new CreateUserCommand({ UserName: name }));
    await iamClient.send(new AttachUserPolicyCommand({ UserName: name, PolicyArn: policyArn }));
    const accessKeyResponse = await iamClient.send(new CreateAccessKeyCommand({ UserName: name }));
    const { AccessKeyId, SecretAccessKey } = accessKeyResponse.AccessKey;
    const s3Client = new S3Client(
        getConfig('default', {
            credentials: {
                accessKeyId: AccessKeyId,
                secretAccessKey: SecretAccessKey,
            },
        }),
    );
    async function deleteUser() {
        await iamClient.send(new DeleteAccessKeyCommand({ UserName: name, AccessKeyId }));
        await iamClient.send(new DetachUserPolicyCommand({ UserName: name, PolicyArn: policyArn }));
        await iamClient.send(new DeletePolicyCommand({ PolicyArn: policyArn }));
        await iamClient.send(new DeleteUserCommand({ UserName: name }));
    }
    return { s3Client, deleteUser };
}

module.exports = {
    makeIamClient,
    createUserWithPolicy,
};
