const assert = require('assert');
const sinon = require('sinon');
const { errors, models, storage } = require('@scality/arsenal');
const AuthInfo = require('@scality/arsenal').auth.AuthInfo;

const notificationDestinationCreate = require('../../../lib/api/notificationDestinationCreate');
const recordStore = require('../../../lib/api/apiUtils/notificationDestination/recordStore');
const { config } = require('../../../lib/Config');
const constants = require('../../../constants');
const { cleanup, DummyRequestLogger, makeAuthInfo } = require('../helpers');

const { metadata } = storage.metadata.inMemory.metadata;

const { accountResourcesBucket: recordsBucket, notificationDestinationKeyTag: tag, splitter } = constants;
// the account of accessKey1 in makeAuthInfo
const accountId = '123456789098';
const keyOf = name => `${accountId}${splitter}${tag}${splitter}${name}`;

const saslSslBody =
    '<NotificationDestination>' +
    '<Type>kafka</Type>' +
    '<Connection>' +
    '<Brokers><Broker>broker1.example.com:9093</Broker></Brokers><Topic>bucket-events</Topic>' +
    '<RequiredAcks>-1</RequiredAcks><CompressionType>zstd</CompressionType>' +
    '</Connection>' +
    '<Auth>' +
    '<Type>sasl_ssl</Type>' +
    '<Sasl><Mechanism>SCRAM-SHA-512</Mechanism><Username>sentinel-user-7f3a</Username><Password>sentinel-pass-7f3a</Password></Sasl>' +
    '<Tls><CaCertificate>-----BEGIN CERTIFICATE-----...</CaCertificate></Tls>' +
    '</Auth>' +
    '</NotificationDestination>';

const accountAuthInfo = makeAuthInfo('accessKey1');

// An identity of the account whose short ID is not the account ID, as Vault
// gives for an IAM user or a role. makeAuthInfo gives a user the short ID of
// its account.
function makeIdentity(arn, shortid) {
    return new AuthInfo({ canonicalID: accountAuthInfo.getCanonicalID(), shortid, arn });
}

const userAuthInfo = makeIdentity(`arn:aws:iam::${accountId}:user/alice`, 'AIDAUSERID0000000000');

function makeRequest(name, post) {
    return {
        method: 'PUT',
        url: `/?notification-destination&name=${encodeURIComponent(name)}`,
        query: { 'notification-destination': '', name },
        headers: { host: 's3.amazonaws.com' },
        post,
        actionImplicitDenies: false,
    };
}

// Resolve with the callback arguments, so that a failed assertion fails the
// test at once instead of in a metadata callback.
function create(authInfo, request, log) {
    return new Promise(resolve => notificationDestinationCreate(authInfo, request, log, (...args) => resolve(args)));
}

function storedRecord(name) {
    const keys = metadata.keyMaps.get(recordsBucket);
    return keys ? keys.get(keyOf(name)) : undefined;
}

describe('notificationDestinationCreate', () => {
    let log;
    let savedGlobalDestinations;
    let savedBackend;

    // The tests read the in-memory store, so the record store runs on mem,
    // whatever S3BACKEND says. stop() drops its client after each test.
    beforeEach(() => {
        savedBackend = config.backends.metadata;
        config.backends.metadata = 'mem';
        recordStore.stop();
        cleanup();
        log = new DummyRequestLogger();
        savedGlobalDestinations = config.bucketNotificationDestinations;
        config.bucketNotificationDestinations = [{ resource: 'target1', type: 'dummy', host: 'localhost:6000' }];
    });

    afterEach(() => {
        recordStore.stop();
        config.backends.metadata = savedBackend;
        config.bucketNotificationDestinations = savedGlobalDestinations;
        sinon.restore();
    });

    it('should store the record and answer with no body', async () => {
        const before = Date.now();
        const args = await create(accountAuthInfo, makeRequest('orders', saslSslBody), log);
        assert.strictEqual(args.length, 1, 'Create answers callback(err)');
        assert.ifError(args[0]);

        const record = storedRecord('orders');
        assert(record, 'the record should be stored');
        const { CreatedAt, UpdatedAt, ...rest } = record;
        assert.deepStrictEqual(rest, {
            modelVersion: 1,
            Name: 'orders',
            AccountId: accountId,
            Arn: `arn:scality:bucketnotif::${accountId}:orders`,
            Type: 'kafka',
            Connection: {
                Brokers: ['broker1.example.com:9093'],
                Topic: 'bucket-events',
                RequiredAcks: -1,
                CompressionType: 'zstd',
            },
            Auth: {
                Type: 'sasl_ssl',
                Sasl: { Mechanism: 'SCRAM-SHA-512', Username: 'sentinel-user-7f3a', Password: 'sentinel-pass-7f3a' },
                Tls: { CaCertificate: '-----BEGIN CERTIFICATE-----...' },
            },
        });
        assert.strictEqual(CreatedAt, UpdatedAt);
        assert.match(CreatedAt, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/);
        assert(Date.parse(CreatedAt) >= before);
    });

    it('should file the record under the account of an IAM user, never its user ID', async () => {
        const [err] = await create(userAuthInfo, makeRequest('orders', saslSslBody), log);
        assert.ifError(err);
        const record = storedRecord('orders');
        assert(record, 'the record should be stored under the account');
        assert.strictEqual(record.AccountId, accountId);
        assert.strictEqual(record.Arn, `arn:scality:bucketnotif::${accountId}:orders`);
    });

    it('should file the record under the account of an assumed role', async () => {
        const roleAuthInfo = makeIdentity(
            `arn:aws:sts::${accountId}:assumed-role/writer/session1`,
            'AROAROLEID000000000:session1',
        );
        const [err] = await create(roleAuthInfo, makeRequest('orders', saslSslBody), log);
        assert.ifError(err);
        assert.strictEqual(storedRecord('orders')?.AccountId, accountId);
    });

    it('should answer DestinationAlreadyExists to a second Create, and keep the first record', async () => {
        const [firstErr] = await create(accountAuthInfo, makeRequest('orders', saslSslBody), log);
        assert.ifError(firstErr);
        const first = storedRecord('orders');
        const otherBody = saslSslBody.replace('bucket-events', 'other-events');
        const [err] = await create(accountAuthInfo, makeRequest('orders', otherBody), log);
        assert.strictEqual(err?.message, 'DestinationAlreadyExists');
        assert.strictEqual(err.code, 409);
        assert.deepStrictEqual(storedRecord('orders'), first);
    });

    it('should answer DestinationAlreadyExists to the name of a global destination', async () => {
        const [err] = await create(accountAuthInfo, makeRequest('target1', saslSslBody), log);
        assert.strictEqual(err?.message, 'DestinationAlreadyExists');
        assert.strictEqual(storedRecord('target1'), undefined);
    });

    it('should accept any name when no global destination is configured', async () => {
        config.bucketNotificationDestinations = undefined;
        const [err] = await create(accountAuthInfo, makeRequest('target1', saslSslBody), log);
        assert.ifError(err);
        assert(storedRecord('target1'), 'the record should be stored');
    });

    const { nameMaxLength } = models.NotificationDestination;
    ['bad/name', 'a'.repeat(nameMaxLength + 1), 'na:me', '*'].forEach(name => {
        it(`should answer InvalidDestination to the name '${name.slice(0, 20)}'`, async () => {
            const [err] = await create(accountAuthInfo, makeRequest(name, saslSslBody), log);
            assert.strictEqual(err?.message, 'InvalidDestination');
            assert.match(err.description, new RegExp(`has 1 to ${nameMaxLength} letters`));
            assert.strictEqual(metadata.keyMaps.get(recordsBucket), undefined);
        });
    });

    it('should check the name before the body', async () => {
        const [err] = await create(accountAuthInfo, makeRequest('bad/name', ''), log);
        assert.strictEqual(err?.message, 'InvalidDestination');
    });

    [undefined, '', '<NotificationDestination>', 'not xml at all <'].forEach(post => {
        it(`should answer MalformedXML to the body ${JSON.stringify(post)}`, async () => {
            const [err] = await create(accountAuthInfo, makeRequest('orders', post), log);
            assert.strictEqual(err?.message, 'MalformedXML');
            assert.strictEqual(storedRecord('orders'), undefined);
        });
    });

    it('should answer InvalidDestination to a body that the checker refuses, and store nothing', async () => {
        const body = saslSslBody.replace('broker1.example.com:9093', 'broker1.example.com:0');
        const [err] = await create(accountAuthInfo, makeRequest('orders', body), log);
        assert.strictEqual(err?.message, 'InvalidDestination');
        assert.match(err.description, /^Connection\.Brokers\.Broker /);
        assert.strictEqual(storedRecord('orders'), undefined);
    });

    it('should refuse the GSSAPI mechanism', async () => {
        const body = saslSslBody.replace('SCRAM-SHA-512', 'GSSAPI');
        const [err] = await create(accountAuthInfo, makeRequest('orders', body), log);
        assert.strictEqual(err?.message, 'InvalidDestination');
        assert.match(err.description, /Sasl\.Mechanism/);
    });

    it('should pass a record store read error through, and write nothing', async () => {
        sinon.stub(recordStore, 'getDestination').callsArgWith(3, errors.InternalError);
        const put = sinon.spy(recordStore, 'putDestination');
        const [err] = await create(accountAuthInfo, makeRequest('orders', saslSslBody), log);
        assert.strictEqual(err?.message, 'InternalError');
        assert.strictEqual(put.called, false);
    });

    it('should pass a record store write error through', async () => {
        sinon.stub(recordStore, 'putDestination').callsArgWith(4, errors.InternalError);
        const [err] = await create(accountAuthInfo, makeRequest('orders', saslSslBody), log);
        assert.strictEqual(err?.message, 'InternalError');
    });

    it('should never log the record', async () => {
        const spyLog = new DummyRequestLogger();
        ['trace', 'debug', 'info', 'warn', 'error'].forEach(level => sinon.spy(spyLog, level));
        await create(accountAuthInfo, makeRequest('orders', saslSslBody), spyLog);
        await create(accountAuthInfo, makeRequest('orders', saslSslBody), spyLog);
        const logged = JSON.stringify(['trace', 'debug', 'info', 'warn', 'error'].flatMap(level => spyLog[level].args));
        ['sentinel-pass-7f3a', 'sentinel-user-7f3a', 'broker1', 'CERTIFICATE'].forEach(value => {
            assert.doesNotMatch(logged, new RegExp(value));
        });
        assert.match(logged, /orders/);
        assert.match(logged, new RegExp(accountId));
    });
});
