const assert = require('assert');
const crypto = require('crypto');
const path = require('path');
const { execFile } = require('child_process');
const { promisify } = require('util');

const {
    IAMClient,
    GetUserCommand,
    GetUserPolicyCommand,
    ListAccessKeysCommand,
    DeleteUserCommand,
    DeleteUserPolicyCommand,
    DeleteAccessKeyCommand,
    CreateUserCommand,
    NoSuchEntityException,
} = require('@aws-sdk/client-iam');

const { getCredentials } = require('../aws-node-sdk/test/support/credentials');

const execFileAsync = promisify(execFile);

const script = path.join(__dirname, '../../../bin/ensureServiceUser');
const iamEndpoint = process.env.IAM_ENDPOINT || 'http://localhost:8600';
const { accessKeyId, secretAccessKey } = getCredentials();

const systemPrefix = '/scality-internal/';

const iamClient = new IAMClient({
    endpoint: iamEndpoint,
    region: 'us-east-1',
    credentials: {
        accessKeyId,
        secretAccessKey,
    },
});

function randomUserName() {
    return `ensure-service-user-test-${crypto.randomBytes(4).toString('hex')}`;
}

function runScript(userName) {
    return execFileAsync('node', [script, 'apply', userName, '--iam-endpoint', iamEndpoint], {
        env: {
            ...process.env,
            AWS_ACCESS_KEY_ID: accessKeyId,
            AWS_SECRET_ACCESS_KEY: secretAccessKey,
        },
    });
}

async function ignoreNoSuchEntity(promise) {
    try {
        return await promise;
    } catch (err) {
        if (err instanceof NoSuchEntityException) {
            return null;
        }
        throw err;
    }
}

// the cleanup runs whatever state the test left behind, so every
// deletion has to tolerate resources that were never created
async function deleteServiceUser(userName) {
    const keys = await ignoreNoSuchEntity(
        iamClient.send(
            new ListAccessKeysCommand({
                UserName: userName,
                MaxItems: 100,
            }),
        ),
    );
    for (const key of keys ? keys.AccessKeyMetadata : []) {
        await ignoreNoSuchEntity(
            iamClient.send(
                new DeleteAccessKeyCommand({
                    UserName: userName,
                    AccessKeyId: key.AccessKeyId,
                }),
            ),
        );
    }
    await ignoreNoSuchEntity(
        iamClient.send(
            new DeleteUserPolicyCommand({
                UserName: userName,
                PolicyName: userName,
            }),
        ),
    );
    await ignoreNoSuchEntity(
        iamClient.send(
            new DeleteUserCommand({
                UserName: userName,
            }),
        ),
    );
}

describe('ensureServiceUser script', () => {
    let userName;

    beforeEach(() => {
        userName = randomUserName();
    });

    afterEach(async () => {
        await deleteServiceUser(userName);
    });

    it('should create the service user, policy and access key when none exist', async () => {
        const { stdout } = await runScript(userName);

        assert.notStrictEqual(stdout, '');
        assert.ok(!stdout.includes('"level":"error"'), `unexpected error in output: ${stdout}`);

        const result = JSON.parse(stdout);
        assert.strictEqual(result.message, 'success');
        assert.ok(result.data.AccessKeyId);
        assert.ok(result.data.SecretAccessKey);
        assert.strictEqual(result.data.UserName, userName);

        const user = await iamClient.send(new GetUserCommand({ UserName: userName }));
        assert.strictEqual(user.User.Path, systemPrefix);

        const policy = await iamClient.send(
            new GetUserPolicyCommand({
                UserName: userName,
                PolicyName: userName,
            }),
        );
        const policyDocument = JSON.parse(decodeURIComponent(policy.PolicyDocument));
        assert.strictEqual(policyDocument.Statement[0].Sid, 'RateLimitAdminAPIs');

        const keys = await iamClient.send(
            new ListAccessKeysCommand({
                UserName: userName,
                MaxItems: 100,
            }),
        );
        assert.strictEqual(keys.AccessKeyMetadata.length, 1);
        assert.strictEqual(keys.AccessKeyMetadata[0].AccessKeyId, result.data.AccessKeyId);
    });

    it('should succeed without creating a new access key when the user already exists', async () => {
        const first = JSON.parse((await runScript(userName)).stdout);
        const { stdout } = await runScript(userName);

        assert.ok(!stdout.includes('"level":"error"'), `unexpected error in output: ${stdout}`);

        const result = JSON.parse(stdout);
        assert.strictEqual(result.message, 'success');
        // on re-run the script reports the existing key metadata instead of creating one
        assert.strictEqual(result.data.length, 1);
        assert.strictEqual(result.data[0].AccessKeyId, first.data.AccessKeyId);

        const keys = await iamClient.send(
            new ListAccessKeysCommand({
                UserName: userName,
                MaxItems: 100,
            }),
        );
        assert.strictEqual(keys.AccessKeyMetadata.length, 1);
    });

    it('should fail when the user exists outside the scality-internal path', async () => {
        await iamClient.send(new CreateUserCommand({ UserName: userName }));

        await assert.rejects(runScript(userName), err => {
            assert.strictEqual(err.code, 1);
            assert.match(err.stdout, /EntityAlreadyExists/);
            return true;
        });
    });
});
