const assert = require('assert');
const crypto = require('crypto');
const {
    IAMClient,
    CreateUserCommand,
    CreateAccessKeyCommand,
    CreatePolicyCommand,
    AttachUserPolicyCommand,
    DetachUserPolicyCommand,
    DeletePolicyCommand,
    DeleteAccessKeyCommand,
    DeleteUserCommand,
    ListAccessKeysCommand,
    ListAttachedUserPoliciesCommand,
    ListPoliciesCommand,
} = require('@aws-sdk/client-iam');

const { makeRequest, makeS3Request } = require('../../utils/makeRequest');
const { getCredentials } = require('../../../aws-node-sdk/test/support/credentials');
const getConfig = require('../../../aws-node-sdk/test/support/config');
const { config } = require('../../../../../lib/Config');

const ipAddress = process.env.IP ? process.env.IP : 'localhost';
const { accessKeyId, secretAccessKey } = getCredentials();
const authCredentials = { accessKey: accessKeyId, secretKey: secretAccessKey };

// The router gives these answers before it reads the destination API flag.
// Thus the answers are the same when the flag is on or off. The test never
// creates the bucket, because the router answers before any bucket lookup.
const bucket = 'notification-destination-routes';

function makeDestinationRequest(method, path, callback) {
    makeRequest(
        {
            authCredentials,
            hostname: ipAddress,
            port: 8000,
            method,
            path,
            queryObj: { 'notification-destination': '' },
        },
        callback,
    );
}

describe('notification destination routes', () => {
    ['GET', 'PUT', 'DELETE'].forEach(method => {
        it(`should answer MethodNotAllowed to ${method} on a bucket`, done => {
            makeDestinationRequest(method, `/${bucket}`, (err, res) => {
                assert(err, 'expected an error');
                assert.strictEqual(err.code, 'MethodNotAllowed');
                assert.strictEqual(err.statusCode, 405);
                assert.strictEqual(res.statusCode, 405);
                done();
            });
        });
    });

    it('should answer 405 to HEAD on a bucket', done => {
        makeDestinationRequest('HEAD', `/${bucket}`, (err, res) => {
            // A HEAD answer has no body, so there is no error to parse from it.
            assert.ifError(err);
            assert.strictEqual(res.statusCode, 405);
            done();
        });
    });

    ['PUT', 'POST', 'DELETE'].forEach(method => {
        it(`should answer InvalidArgument to ${method} on the service with no name`, done => {
            makeDestinationRequest(method, '/', (err, res) => {
                assert(err, 'expected an error');
                assert.strictEqual(err.code, 'InvalidArgument');
                assert.strictEqual(err.statusCode, 400);
                assert.strictEqual(res.statusCode, 400);
                done();
            });
        });
    });
});

// The destination calls follow the flag as the server reads it. The CI job
// that enables the flag gives the test runner the same environment and
// config as the server.
const destinationApiOn = config.notificationDestinationApi.enabled;
const describeOn = destinationApiOn ? describe : describe.skip;
const describeOff = destinationApiOn ? describe.skip : describe;
// The IAM cases need a real Vault: the in-memory one skips IAM policies.
const describeIam = destinationApiOn && config.backends.auth !== 'mem' ? describe : describe.skip;

// The IAM endpoint of Vault. IAM_ENDPOINT comes first, as in
// tests/functional/scripts/ensureServiceUser.js.
function iamEndpoint() {
    return process.env.IAM_ENDPOINT || `http://${config.vaultd?.host || 'localhost'}:8600`;
}

function makeIamClient() {
    const iamConfig = getConfig('default', { region: 'us-east-1' });
    iamConfig.endpoint = iamEndpoint();
    return new IAMClient(iamConfig);
}

const policyNameOf = userName => `${userName}-policy`;

// Run one clean-up step. An entity that does not exist is fine. Log any
// other error and go on, so that one failed step never leaves the rest of
// the IAM caller behind.
async function cleanUpStep(what, step) {
    try {
        await step();
    } catch (err) {
        if (err.name !== 'NoSuchEntity') {
            process.stderr.write(`IAM clean-up: could not ${what}: ${err.name}: ${err.message}\n`);
        }
    }
}

/**
 * Remove the IAM caller that createIamCaller makes, or what a setup that
 * stopped halfway left of it: its access keys, its policy, then the user.
 * Each step lists what exists.
 * @param {IAMClient} iamClient - the IAM client of Vault
 * @param {string} userName - the name of the IAM user
 * @return {Promise<undefined>} - resolves once every step ran
 */
async function removeIamCaller(iamClient, userName) {
    let keys = [];
    await cleanUpStep(`list the access keys of ${userName}`, async () => {
        const res = await iamClient.send(new ListAccessKeysCommand({ UserName: userName }));
        keys = res.AccessKeyMetadata || [];
    });
    for (const { AccessKeyId } of keys) {
        await cleanUpStep(`delete the access key ${AccessKeyId}`, () =>
            iamClient.send(new DeleteAccessKeyCommand({ UserName: userName, AccessKeyId })),
        );
    }
    let attached = [];
    await cleanUpStep(`list the policies of ${userName}`, async () => {
        const res = await iamClient.send(new ListAttachedUserPoliciesCommand({ UserName: userName }));
        attached = res.AttachedPolicies || [];
    });
    for (const { PolicyArn } of attached) {
        await cleanUpStep(`detach the policy ${PolicyArn}`, () =>
            iamClient.send(new DetachUserPolicyCommand({ UserName: userName, PolicyArn })),
        );
    }
    // A setup can stop after it creates the policy and before it attaches it.
    // Thus look for the policy by its name too.
    const named = [];
    await cleanUpStep(`list the policy of ${userName}`, async () => {
        let marker;
        do {
            const res = await iamClient.send(new ListPoliciesCommand({ Scope: 'Local', Marker: marker }));
            named.push(...(res.Policies || []).filter(policy => policy.PolicyName === policyNameOf(userName)));
            marker = res.IsTruncated ? res.Marker : undefined;
        } while (marker);
    });
    const policyArns = new Set([...attached.map(policy => policy.PolicyArn), ...named.map(policy => policy.Arn)]);
    for (const policyArn of policyArns) {
        await cleanUpStep(`delete the policy ${policyArn}`, () =>
            iamClient.send(new DeletePolicyCommand({ PolicyArn: policyArn })),
        );
    }
    await cleanUpStep(`delete the user ${userName}`, () =>
        iamClient.send(new DeleteUserCommand({ UserName: userName })),
    );
}

/**
 * Create an IAM user of the test account with an access key. When the test
 * gives an action and a resource, the user gets a policy that allows that
 * action on that resource only. Any leftover of that user goes first.
 * @param {IAMClient} iamClient - the IAM client of Vault
 * @param {string} userName - the name of the IAM user
 * @param {object} [allow] - { action, resource } of the policy
 * @return {Promise<object>} - the credentials that sign the requests of the
 * user
 */
async function createIamCaller(iamClient, userName, allow) {
    await removeIamCaller(iamClient, userName);
    await iamClient.send(new CreateUserCommand({ UserName: userName }));
    if (allow) {
        const { Policy } = await iamClient.send(
            new CreatePolicyCommand({
                PolicyName: policyNameOf(userName),
                PolicyDocument: JSON.stringify({
                    Version: '2012-10-17',
                    Statement: [{ Effect: 'Allow', Action: [allow.action], Resource: [allow.resource] }],
                }),
            }),
        );
        await iamClient.send(new AttachUserPolicyCommand({ UserName: userName, PolicyArn: Policy.Arn }));
    }
    const { AccessKey } = await iamClient.send(new CreateAccessKeyCommand({ UserName: userName }));
    return { accessKey: AccessKey.AccessKeyId, secretKey: AccessKey.SecretAccessKey };
}

const saslSslBody =
    '<NotificationDestination>' +
    '<Type>kafka</Type>' +
    '<Connection>' +
    '<Brokers><Broker>broker1.example.com:9093</Broker></Brokers><Topic>bucket-events</Topic>' +
    '<RequiredAcks>-1</RequiredAcks><CompressionType>zstd</CompressionType>' +
    '</Connection>' +
    '<Auth>' +
    '<Type>sasl_ssl</Type>' +
    '<Sasl><Mechanism>SCRAM-SHA-512</Mechanism><Username>events</Username><Password>secret</Password></Sasl>' +
    '<Tls><CaCertificate>-----BEGIN CERTIFICATE-----...</CaCertificate></Tls>' +
    '</Auth>' +
    '</NotificationDestination>';

// No Delete exists yet to remove what a run creates, so each run uses names
// of its own.
const runId = crypto.randomBytes(4).toString('hex');
const uniqueName = prefix => `${prefix}-${runId}`;

function destinationRequest(method, queryObj, requestBody, credentials = authCredentials) {
    return new Promise(resolve =>
        makeS3Request(
            {
                method,
                authCredentials: credentials,
                queryObj: { 'notification-destination': '', ...queryObj },
                requestBody,
            },
            (err, res) => resolve({ err, res }),
        ),
    );
}

const createDestination = (name, body, credentials) => destinationRequest('PUT', { name }, body, credentials);
const getDestination = (name, credentials) => destinationRequest('GET', { name }, undefined, credentials);

const escapeRegExp = text => text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

// The text of each element of that name in an XML body.
function elementValues(body, element) {
    return [...body.matchAll(new RegExp(`<${element}>([^<]*)</${element}>`, 'g'))].map(match => match[1]);
}

function assertError({ err, res }, code, statusCode) {
    assert(err, `expected ${code}, got ${res?.statusCode}`);
    assert.strictEqual(err.code, code);
    assert.strictEqual(res?.statusCode, statusCode);
}

// A destination name that no other test of the run uses. Thus an IAM test
// creates destinations of its own and reads none that another test left.
let nameSequence = 0;
const testName = prefix => uniqueName(`${prefix}-${++nameSequence}`);

// Create a destination of the test account, and return its Arn: the Arn
// that Get answers is the resource that a policy names.
async function createDestinationArn(name) {
    assert.ifError((await createDestination(name, saslSslBody)).err);
    const { err, res } = await getDestination(name);
    assert.ifError(err);
    return elementValues(res.body, 'Arn')[0];
}

describeOff('notification destination calls with the API off', () => {
    [
        ['CreateNotificationDestination', 'PUT', { name: 'orders' }, saslSslBody],
        ['UpdateNotificationDestination', 'POST', { name: 'orders' }, saslSslBody],
        ['GetNotificationDestination', 'GET', { name: 'orders' }],
        ['DeleteNotificationDestination', 'DELETE', { name: 'orders' }],
        ['ListNotificationDestinations', 'GET', {}],
    ].forEach(([operation, method, query, body]) => {
        it(`should answer NotImplemented to ${operation}`, async () => {
            assertError(await destinationRequest(method, query, body), 'NotImplemented', 501);
        });
    });
});

describeOn('CreateNotificationDestination', () => {
    it('should create a destination and answer 200 with no body', async () => {
        const mtlsBody = saslSslBody.replace(
            /<Auth>.*<\/Auth>/,
            '<Auth><Type>mtls</Type><Tls><ClientCertificate>cert</ClientCertificate>' +
                '<ClientKey>key</ClientKey></Tls></Auth>',
        );
        const { err, res } = await createDestination(uniqueName('orders'), mtlsBody);
        assert.ifError(err);
        assert.strictEqual(res.statusCode, 200);
        assert.strictEqual(res.body, '');
    });

    it('should answer DestinationAlreadyExists to a second Create of the name', async () => {
        const name = uniqueName('twice');
        const first = await createDestination(name, saslSslBody);
        assert.ifError(first.err);
        assertError(await createDestination(name, saslSslBody), 'DestinationAlreadyExists', 409);
    });

    it('should answer DestinationAlreadyExists to the name of a global destination', async function globalName() {
        const globalDestination = (config.bucketNotificationDestinations || [])[0];
        if (!globalDestination) {
            // The unit tests cover this case. This config has no global destination.
            this.skip();
        }
        assertError(await createDestination(globalDestination.resource, saslSslBody), 'DestinationAlreadyExists', 409);
    });

    it('should answer AccessDenied to an anonymous request', async () => {
        assertError(await createDestination(uniqueName('anonymous'), saslSslBody, null), 'AccessDenied', 403);
    });

    it('should answer InvalidDestination to a name outside the grammar', async () => {
        assertError(await createDestination('bad/name', saslSslBody), 'InvalidDestination', 400);
    });

    [
        ['a Type other than kafka', ['<Type>kafka</Type>', '<Type>rabbitmq</Type>']],
        ['a broker with a space', ['broker1.example.com:9093', 'bad host:9093']],
        [
            '11 brokers',
            [/<Brokers>.*<\/Brokers>/, `<Brokers>${'<Broker>broker1.example.com:9093</Broker>'.repeat(11)}</Brokers>`],
        ],
        ['a broker with port 0', ['broker1.example.com:9093', 'broker1.example.com:0']],
        ['an IPv6 broker with no brackets', ['broker1.example.com:9093', 'fd00::1:9093']],
        ['a loopback IPv4 broker', ['broker1.example.com:9093', '127.0.0.1:9092']],
        ['a loopback IPv6 broker', ['broker1.example.com:9093', '[::1]:9092']],
        ['a link-local broker', ['broker1.example.com:9093', '169.254.169.254:80']],
        ['an unspecified broker', ['broker1.example.com:9093', '0.0.0.0:9092']],
        ['the old Host and Port', [/<Brokers>.*<\/Brokers>/, '<Host>broker1.example.com</Host><Port>9093</Port>']],
        ['a Topic with a space', ['bucket-events', 'bucket events']],
        ['a Topic of 250 letters', ['bucket-events', 'a'.repeat(250)]],
        ['the Topic ..', ['bucket-events', '..']],
        ['a SASL type with no Sasl block', [/<Sasl>.*<\/Sasl>/, '']],
        ['the GSSAPI mechanism', ['SCRAM-SHA-512', 'GSSAPI']],
        [
            'mtls with no client key',
            [
                /<Auth>.*<\/Auth>/,
                '<Auth><Type>mtls</Type><Tls><ClientCertificate>cert</ClientCertificate></Tls></Auth>',
            ],
        ],
    ].forEach(([what, [from, to]]) => {
        it(`should answer InvalidDestination to ${what}`, async () => {
            const body = saslSslBody.replace(from, to);
            assert.notStrictEqual(body, saslSslBody);
            assertError(await createDestination(uniqueName('invalid'), body), 'InvalidDestination', 400);
        });
    });

    [
        ['an empty body', undefined],
        ['a broken body', '<NotificationDestination><Type>kafka'],
    ].forEach(([what, body]) => {
        it(`should answer MalformedXML to ${what}`, async () => {
            assertError(await createDestination(uniqueName('malformed'), body), 'MalformedXML', 400);
        });
    });

    it('should accept the sasl_ssl example with a Tls CA', async () => {
        const { err, res } = await createDestination(uniqueName('example'), saslSslBody);
        assert.ifError(err);
        assert.strictEqual(res.statusCode, 200);
    });

    it('should accept a broker at a private address', async () => {
        const body = saslSslBody.replace('broker1.example.com:9093', '10.1.2.3:9092');
        assert.notStrictEqual(body, saslSslBody);
        const { err, res } = await createDestination(uniqueName('private'), body);
        assert.ifError(err);
        assert.strictEqual(res.statusCode, 200);
    });
});

describeOn('GetNotificationDestination', () => {
    // Strings that only a secret field holds, so that finding one in a body
    // means a secret leaked.
    const secrets = {
        password: 'sentinel-pass-3e8d',
        clientKey: 'sentinel-key-3e8d',
        clientKeyPassword: 'sentinel-keypass-3e8d',
    };
    const brokers = ['broker2.example.com:9093', 'broker1.example.com:9094'];
    const connection =
        '<Connection>' +
        `<Brokers>${brokers.map(broker => `<Broker>${broker}</Broker>`).join('')}</Brokers>` +
        '<Topic>bucket-events</Topic><RequiredAcks>1</RequiredAcks><CompressionType>lz4</CompressionType>' +
        '</Connection>';
    const bodies = {
        // eslint-disable-next-line camelcase
        sasl_ssl:
            `<NotificationDestination><Type>kafka</Type>${connection}` +
            '<Auth><Type>sasl_ssl</Type>' +
            `<Sasl><Mechanism>SCRAM-SHA-256</Mechanism><Username>events</Username><Password>${secrets.password}</Password></Sasl>` +
            '<Tls><CaCertificate>ca-cert</CaCertificate></Tls>' +
            '</Auth></NotificationDestination>',
        mtls:
            `<NotificationDestination><Type>kafka</Type>${connection}` +
            '<Auth><Type>mtls</Type><Tls><ClientCertificate>client-cert</ClientCertificate>' +
            `<ClientKey>${secrets.clientKey}</ClientKey><ClientKeyPassword>${secrets.clientKeyPassword}</ClientKeyPassword>` +
            '</Tls></Auth></NotificationDestination>',
    };

    Object.keys(bodies).forEach(authType => {
        it(`should answer every field of a ${authType} destination, and no secret`, async () => {
            const name = uniqueName(`get-${authType}`);
            const created = await createDestination(name, bodies[authType]);
            assert.ifError(created.err);
            const { err, res } = await getDestination(name);
            assert.ifError(err);
            assert.strictEqual(res.statusCode, 200);
            const { body } = res;
            assert.match(body, /^<\?xml version="1\.0" encoding="UTF-8"\?><NotificationDestination>/);
            assert.deepStrictEqual(elementValues(body, 'Name'), [name]);
            const [accountId] = elementValues(body, 'AccountId');
            assert.match(accountId, /^[0-9]{12}$/);
            assert.deepStrictEqual(elementValues(body, 'Arn'), [`arn:scality:bucketnotif::${accountId}:${name}`]);
            assert.match(
                body,
                new RegExp(
                    escapeRegExp(`<Type>kafka</Type>${connection}<Auth><Type>${authType}</Type></Auth><CreatedAt>`),
                ),
            );
            Object.values(secrets).forEach(secret => assert.doesNotMatch(body, new RegExp(secret)));
            assert.doesNotMatch(body, /<Sasl>|<Tls>|<Username>|<Password>|<ClientKey>|client-cert|ca-cert/);
        });
    });

    it('should answer the brokers in the order they were created', async () => {
        const name = uniqueName('get-brokers');
        assert.ifError((await createDestination(name, bodies.mtls)).err);
        const { err, res } = await getDestination(name);
        assert.ifError(err);
        assert.deepStrictEqual(elementValues(res.body, 'Broker'), brokers);
    });

    it('should answer CreatedAt and UpdatedAt in ISO 8601 UTC, to the millisecond', async () => {
        const name = uniqueName('get-times');
        const before = Date.now();
        assert.ifError((await createDestination(name, bodies.mtls)).err);
        const { err, res } = await getDestination(name);
        assert.ifError(err);
        const [createdAt] = elementValues(res.body, 'CreatedAt');
        const [updatedAt] = elementValues(res.body, 'UpdatedAt');
        assert.match(createdAt, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/);
        assert.strictEqual(updatedAt, createdAt);
        assert(Date.parse(createdAt) >= before, `${createdAt} is before the Create`);
        assert(Date.parse(createdAt) <= Date.now(), `${createdAt} is after the Get`);
    });

    it('should answer NoSuchDestination to an unknown name', async () => {
        assertError(await getDestination(uniqueName('unknown')), 'NoSuchDestination', 404);
    });

    it('should answer InvalidDestination to a name outside the grammar', async () => {
        assertError(await getDestination('bad/name'), 'InvalidDestination', 400);
    });

    it('should answer AccessDenied to an anonymous request', async () => {
        assertError(await getDestination(uniqueName('anonymous'), null), 'AccessDenied', 403);
    });
});

describeOn('ListNotificationDestinations', () => {
    const listDestinations = (queryObj, credentials) => destinationRequest('GET', queryObj, undefined, credentials);
    const listedNames = body =>
        [...body.matchAll(/<NotificationDestination><Name>([^<]*)<\/Name>/g)].map(match => match[1]);
    // The other tests of this file create names of their own in the same
    // account and never delete them. Thus the names here share one prefix
    // that no other test uses, and each page starts after that prefix. Each
    // test gets a prefix and destinations of its own.
    const lisa = getCredentials('lisa');
    const lisaCredentials = { accessKey: lisa.accessKeyId, secretKey: lisa.secretAccessKey };
    let prefix;
    let names;
    let lisaName;

    beforeEach(async () => {
        prefix = testName('zz-list');
        names = ['a', 'b', 'c'].map(suffix => `${prefix}-${suffix}`);
        lisaName = `${prefix}-lisa`;
        assert.notStrictEqual(lisaCredentials.accessKey, authCredentials.accessKey, 'a second account is needed');
        for (const name of [names[2], names[0], names[1]]) {
            assert.ifError((await createDestination(name, saslSslBody)).err);
        }
        assert.ifError((await createDestination(lisaName, saslSslBody, lisaCredentials)).err);
    });

    it('should page through three names with max-items 2, NextMarker then marker', async () => {
        const first = await listDestinations({ 'max-items': '2', 'marker': `${prefix}-` });
        assert.ifError(first.err);
        assert.strictEqual(first.res.statusCode, 200);
        assert.match(
            first.res.body,
            /^<\?xml version="1\.0" encoding="UTF-8"\?><ListNotificationDestinationsResult><IsTruncated>true<\/IsTruncated><NextMarker>/,
        );
        assert.doesNotMatch(first.res.body, /xmlns/);
        assert.deepStrictEqual(listedNames(first.res.body), names.slice(0, 2));
        assert.deepStrictEqual(elementValues(first.res.body, 'NextMarker'), [names[1]]);

        const second = await listDestinations({ 'max-items': '2', 'marker': names[1] });
        assert.ifError(second.err);
        const listed = listedNames(second.res.body);
        assert.strictEqual(listed[0], names[2]);
        assert.deepStrictEqual(
            listed.filter(name => name.startsWith(prefix)),
            [names[2]],
        );
        // A name that sorts after these may exist. NextMarker follows IsTruncated.
        const [isTruncated] = elementValues(second.res.body, 'IsTruncated');
        assert.deepStrictEqual(
            elementValues(second.res.body, 'NextMarker'),
            isTruncated === 'true' ? [listed[listed.length - 1]] : [],
        );
    });

    it('should answer each entry as Get does, with no secret', async () => {
        const { err, res } = await listDestinations({ 'max-items': '1', 'marker': `${prefix}-` });
        assert.ifError(err);
        const got = await getDestination(names[0]);
        assert.ifError(got.err);
        const entry = got.res.body.replace('<?xml version="1.0" encoding="UTF-8"?>', '');
        assert.match(res.body, new RegExp(escapeRegExp(entry)));
        assert.doesNotMatch(res.body, /<Sasl>|<Tls>|<Username>|<Password>|secret/);
    });

    it('should answer IsTruncated false and no entry after the last name', async () => {
        // No name of at most 128 characters sorts after 128 z.
        const { err, res } = await listDestinations({ marker: 'z'.repeat(128) });
        assert.ifError(err);
        assert.strictEqual(
            res.body,
            '<?xml version="1.0" encoding="UTF-8"?>' +
                '<ListNotificationDestinationsResult><IsTruncated>false</IsTruncated>' +
                '</ListNotificationDestinationsResult>',
        );
    });

    it('should never list the destinations of another account', async () => {
        const mine = await listDestinations({ 'max-items': '1000', 'marker': `${prefix}-` });
        assert.ifError(mine.err);
        const myNames = listedNames(mine.res.body);
        assert(!myNames.includes(lisaName), mine.res.body);
        assert(myNames.includes(names[0]));

        const theirs = await listDestinations({ 'max-items': '1000', 'marker': `${prefix}-` }, lisaCredentials);
        assert.ifError(theirs.err);
        assert.deepStrictEqual(
            listedNames(theirs.res.body).filter(name => name.startsWith(prefix)),
            [lisaName],
        );
        const [lisaAccountId] = elementValues(theirs.res.body, 'AccountId');
        assert.notStrictEqual(lisaAccountId, elementValues(mine.res.body, 'AccountId')[0]);
    });

    ['0', '1001', 'abc', '2.5', '', '+5'].forEach(maxItems => {
        it(`should answer InvalidArgument to max-items '${maxItems}'`, async () => {
            assertError(await listDestinations({ 'max-items': maxItems }), 'InvalidArgument', 400);
        });
    });

    it('should answer AccessDenied to an anonymous request', async () => {
        assertError(await listDestinations({}, null), 'AccessDenied', 403);
    });
});

describeIam('CreateNotificationDestination by an IAM user', () => {
    const iamClient = makeIamClient();
    const userName = uniqueName('destination-no-policy');
    let credentials;

    beforeEach(async () => {
        credentials = await createIamCaller(iamClient, userName);
    });

    afterEach(async () => {
        await removeIamCaller(iamClient, userName);
    });

    it('should answer AccessDenied to a user with no policy', async () => {
        assertError(await createDestination(uniqueName('denied'), saslSslBody, credentials), 'AccessDenied', 403);
    });
});

describeIam('GetNotificationDestination by an IAM user', () => {
    const iamClient = makeIamClient();
    const userName = uniqueName('destination-get-one');
    let allowedName;
    let otherName;
    let credentials;

    beforeEach(async () => {
        allowedName = testName('get-allowed');
        otherName = testName('get-other');
        const allowedArn = await createDestinationArn(allowedName);
        assert.ifError((await createDestination(otherName, saslSslBody)).err);
        credentials = await createIamCaller(iamClient, userName, {
            action: 'scality:GetBucketNotificationDestination',
            resource: allowedArn,
        });
    });

    afterEach(async () => {
        await removeIamCaller(iamClient, userName);
    });

    it('should read the destination that the policy names', async () => {
        const { err, res } = await getDestination(allowedName, credentials);
        assert.ifError(err);
        assert.strictEqual(res.statusCode, 200);
        assert.deepStrictEqual(elementValues(res.body, 'Name'), [allowedName]);
    });

    it('should answer AccessDenied to another destination', async () => {
        assertError(await getDestination(otherName, credentials), 'AccessDenied', 403);
    });
});

describeIam('ListNotificationDestinations by an IAM user', () => {
    const iamClient = makeIamClient();
    // List is authorized on every destination of the account. A policy on
    // the account wildcard allows it. A policy on one destination does not.
    const userNames = {
        wildcard: uniqueName('destination-list-all'),
        one: uniqueName('destination-list-one'),
    };
    let listedName;
    let credentials;

    beforeEach(async () => {
        listedName = testName('list-iam');
        const destinationArn = await createDestinationArn(listedName);
        const resources = {
            wildcard: destinationArn.replace(/[^:]*$/, '*'),
            one: destinationArn,
        };
        credentials = {};
        for (const [kind, userName] of Object.entries(userNames)) {
            credentials[kind] = await createIamCaller(iamClient, userName, {
                action: 'scality:ListBucketNotificationDestinations',
                resource: resources[kind],
            });
        }
    });

    afterEach(async () => {
        for (const userName of Object.values(userNames)) {
            await removeIamCaller(iamClient, userName);
        }
    });

    const listAs = kind =>
        destinationRequest(
            'GET',
            { 'max-items': '1', 'marker': listedName.slice(0, -1) },
            undefined,
            credentials[kind],
        );

    it('should list for a user allowed on every destination of the account', async () => {
        const { err, res } = await listAs('wildcard');
        assert.ifError(err);
        assert.strictEqual(res.statusCode, 200);
        assert.deepStrictEqual(elementValues(res.body, 'Name'), [listedName]);
    });

    it('should answer AccessDenied to a user allowed on one destination only', async () => {
        assertError(await listAs('one'), 'AccessDenied', 403);
    });
});
