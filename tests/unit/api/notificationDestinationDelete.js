const assert = require('assert');
const sinon = require('sinon');
const { errors, models, storage } = require('@scality/arsenal');
const AuthInfo = require('@scality/arsenal').auth.AuthInfo;

const notificationDestinationDelete = require('../../../lib/api/notificationDestinationDelete');
const notificationDestinationGet = require('../../../lib/api/notificationDestinationGet');
const recordStore = require('../../../lib/api/apiUtils/notificationDestination/recordStore');
const { config } = require('../../../lib/Config');
const constants = require('../../../constants');
const { cleanup, DummyRequestLogger, makeAuthInfo } = require('../helpers');

const { metadata } = storage.metadata.inMemory.metadata;

const { accountResourcesBucket: recordsBucket, notificationDestinationKeyTag: tag, splitter } = constants;
// the account of accessKey1 in makeAuthInfo
const accountId = '123456789098';
const keyOf = name => `${accountId}${splitter}${tag}${splitter}${name}`;

// A stored record that each test deletes, with values that only it holds,
// so that finding one in a log means the record leaked.
const storedSaslSsl = {
    modelVersion: 1,
    Name: 'orders',
    AccountId: accountId,
    Arn: `arn:scality:bucketnotif::${accountId}:orders`,
    Type: 'kafka',
    Connection: {
        Brokers: ['broker1.example.com:9093'],
        Topic: 'bucket-events',
        RequiredAcks: 1,
        CompressionType: 'zstd',
    },
    Auth: {
        Type: 'sasl_ssl',
        Sasl: { Mechanism: 'SCRAM-SHA-512', Username: 'sentinel-user-7a3f', Password: 'sentinel-pass-7a3f' },
        Tls: { CaCertificate: 'sentinel-ca-7a3f' },
    },
    CreatedAt: '2020-01-01T00:00:00Z',
    UpdatedAt: '2020-01-01T00:00:00Z',
};

const accountAuthInfo = makeAuthInfo('accessKey1');
// An IAM user whose short ID, the user ID, is not the account ID, as Vault
// gives it. makeAuthInfo gives a user the short ID of its account.
const userAuthInfo = new AuthInfo({
    canonicalID: accountAuthInfo.getCanonicalID(),
    shortid: 'AIDAUSERID0000000000',
    arn: `arn:aws:iam::${accountId}:user/alice`,
});
const otherAuthInfo = makeAuthInfo('accessKey2');

function makeRequest(name, method = 'DELETE') {
    return {
        method,
        url: `/?notification-destination&name=${encodeURIComponent(name)}`,
        query: { 'notification-destination': '', name },
        headers: { host: 's3.amazonaws.com' },
        actionImplicitDenies: false,
    };
}

// Resolve with the callback arguments, so that a failed assertion fails the
// test at once instead of in a metadata callback.
function deleteDestination(authInfo, request, log) {
    return new Promise(resolve => notificationDestinationDelete(authInfo, request, log, (...args) => resolve(args)));
}

function getDestination(authInfo, name, log) {
    return new Promise(resolve =>
        notificationDestinationGet(authInfo, makeRequest(name, 'GET'), log, (...args) => resolve(args)),
    );
}

function seed(record, log) {
    return new Promise((resolve, reject) =>
        recordStore.putDestination(accountId, record.Name, structuredClone(record), log, err =>
            err ? reject(err) : resolve(),
        ),
    );
}

function storedRecord(name) {
    const keys = metadata.keyMaps.get(recordsBucket);
    return keys ? keys.get(keyOf(name)) : undefined;
}

describe('notificationDestinationDelete', () => {
    let log;
    let savedBackend;

    beforeEach(async () => {
        // The tests read the in-memory store, so the record store runs on
        // mem, whatever S3BACKEND says. stop() drops its client after each
        // test.
        savedBackend = config.backends.metadata;
        config.backends.metadata = 'mem';
        recordStore.stop();
        cleanup();
        log = new DummyRequestLogger();
        await seed(storedSaslSsl, log);
    });

    afterEach(() => {
        recordStore.stop();
        config.backends.metadata = savedBackend;
        sinon.restore();
    });

    it('should delete the record and answer with no body, then Get answers NoSuchDestination', async () => {
        const args = await deleteDestination(accountAuthInfo, makeRequest('orders'), log);
        assert.strictEqual(args.length, 1, 'Delete answers callback(err)');
        assert.ifError(args[0]);
        assert.strictEqual(storedRecord('orders'), undefined);

        const [getErr] = await getDestination(accountAuthInfo, 'orders', log);
        assert.strictEqual(getErr?.message, 'NoSuchDestination');
        assert.strictEqual(getErr.code, 404);
    });

    it('should delete only the named record', async () => {
        await seed({ ...storedSaslSsl, Name: 'payments', Arn: `arn:scality:bucketnotif::${accountId}:payments` }, log);
        const [err] = await deleteDestination(accountAuthInfo, makeRequest('orders'), log);
        assert.ifError(err);
        assert.strictEqual(storedRecord('orders'), undefined);
        assert.strictEqual(storedRecord('payments').Name, 'payments');
    });

    it('should delete the destination of the account of an IAM user, never its user ID', async () => {
        const [err] = await deleteDestination(userAuthInfo, makeRequest('orders'), log);
        assert.ifError(err);
        assert.strictEqual(storedRecord('orders'), undefined);
    });

    it('should answer NoSuchDestination to an unknown name, and delete nothing', async () => {
        const del = sinon.spy(recordStore, 'deleteDestination');
        const [err] = await deleteDestination(accountAuthInfo, makeRequest('unknown'), log);
        assert.strictEqual(err?.message, 'NoSuchDestination');
        assert.strictEqual(err.code, 404);
        assert.strictEqual(del.called, false);
    });

    it('should answer NoSuchDestination to a name deleted already', async () => {
        assert.ifError((await deleteDestination(accountAuthInfo, makeRequest('orders'), log))[0]);
        const [err] = await deleteDestination(accountAuthInfo, makeRequest('orders'), log);
        assert.strictEqual(err?.message, 'NoSuchDestination');
    });

    it('should answer NoSuchDestination to the name of another account, and keep the record', async () => {
        const del = sinon.spy(recordStore, 'deleteDestination');
        const [err] = await deleteDestination(otherAuthInfo, makeRequest('orders'), log);
        assert.strictEqual(err?.message, 'NoSuchDestination');
        assert.strictEqual(del.called, false);
        assert.deepStrictEqual(storedRecord('orders'), storedSaslSsl);
    });

    it('should answer NoSuchDestination when the record goes between the read and the delete', async () => {
        // Another Delete of the name may land between the two calls.
        sinon.stub(recordStore, 'deleteDestination').callsArgWith(3, errors.NoSuchDestination);
        const [err] = await deleteDestination(accountAuthInfo, makeRequest('orders'), log);
        assert.strictEqual(err?.message, 'NoSuchDestination');
    });

    ['bad/name', 'a'.repeat(models.NotificationDestination.nameMaxLength + 1), '*', ''].forEach(name => {
        it(`should answer InvalidDestination to the name '${name.slice(0, 20)}', and read nothing`, async () => {
            const get = sinon.spy(recordStore, 'getDestination');
            const del = sinon.spy(recordStore, 'deleteDestination');
            const [err] = await deleteDestination(accountAuthInfo, makeRequest(name), log);
            assert.strictEqual(err?.message, 'InvalidDestination');
            assert.strictEqual(err.code, 400);
            assert.strictEqual(get.called, false);
            assert.strictEqual(del.called, false);
        });
    });

    it('should pass a record store read error through, and delete nothing', async () => {
        sinon.stub(recordStore, 'getDestination').callsArgWith(3, errors.InternalError);
        const del = sinon.spy(recordStore, 'deleteDestination');
        const [err] = await deleteDestination(accountAuthInfo, makeRequest('orders'), log);
        assert.strictEqual(err?.message, 'InternalError');
        assert.strictEqual(del.called, false);
        assert.deepStrictEqual(storedRecord('orders'), storedSaslSsl);
    });

    it('should pass a record store delete error through', async () => {
        sinon.stub(recordStore, 'deleteDestination').callsArgWith(3, errors.InternalError);
        const [err] = await deleteDestination(accountAuthInfo, makeRequest('orders'), log);
        assert.strictEqual(err?.message, 'InternalError');
    });

    it('should never log the record', async () => {
        const spyLog = new DummyRequestLogger();
        const levels = ['trace', 'debug', 'info', 'warn', 'error'];
        levels.forEach(level => sinon.spy(spyLog, level));
        await deleteDestination(otherAuthInfo, makeRequest('orders'), spyLog);
        await deleteDestination(accountAuthInfo, makeRequest('orders'), spyLog);
        await deleteDestination(accountAuthInfo, makeRequest('orders'), spyLog);
        const logged = JSON.stringify(levels.flatMap(level => spyLog[level].args));
        ['sentinel-user-7a3f', 'sentinel-pass-7a3f', 'sentinel-ca-7a3f', 'broker', 'events'].forEach(value =>
            assert.doesNotMatch(logged, new RegExp(value)),
        );
        assert.match(logged, /orders/);
        assert.match(logged, new RegExp(accountId));
    });
});
