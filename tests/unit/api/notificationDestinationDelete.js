const assert = require('assert');
const sinon = require('sinon');
const { errors, models } = require('@scality/arsenal');

const notificationDestinationDelete = require('../../../lib/api/notificationDestinationDelete');
const notificationDestinationGet = require('../../../lib/api/notificationDestinationGet');
const recordStore = require('../../../lib/api/apiUtils/notificationDestination/recordStore');
const { config } = require('../../../lib/Config');
const { DummyRequestLogger } = require('../helpers');
const {
    accountId,
    accountAuthInfo,
    otherAuthInfo,
    userAuthInfo,
    makeRequest,
    call,
    seed,
    storedRecord,
    useMemRecordStore,
} = require('../utils/notificationDestinationHelpers');

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

const remove = (authInfo, name, log) => call(notificationDestinationDelete, authInfo, makeRequest('DELETE', name), log);
const get = (authInfo, name, log) => call(notificationDestinationGet, authInfo, makeRequest('GET', name), log);

describe('notificationDestinationDelete', () => {
    let log;
    let restoreRecordStore;

    beforeEach(async () => {
        restoreRecordStore = useMemRecordStore();
        log = new DummyRequestLogger();
        await seed(storedSaslSsl, log);
    });

    afterEach(() => {
        restoreRecordStore();
        sinon.restore();
    });

    it('should delete the record and answer with no body, then Get answers NoSuchDestination', async () => {
        const args = await remove(accountAuthInfo, 'orders', log);
        assert.strictEqual(args.length, 1, 'Delete answers callback(err)');
        assert.ifError(args[0]);
        assert.strictEqual(storedRecord('orders'), undefined);

        const [getErr] = await get(accountAuthInfo, 'orders', log);
        assert.strictEqual(getErr?.message, 'NoSuchDestination');
        assert.strictEqual(getErr.code, 404);
    });

    it('should delete only the named record', async () => {
        await seed({ ...storedSaslSsl, Name: 'payments', Arn: `arn:scality:bucketnotif::${accountId}:payments` }, log);
        const [err] = await remove(accountAuthInfo, 'orders', log);
        assert.ifError(err);
        assert.strictEqual(storedRecord('orders'), undefined);
        assert.strictEqual(storedRecord('payments').Name, 'payments');
    });

    it('should delete the destination of the account of an IAM user, never its user ID', async () => {
        const [err] = await remove(userAuthInfo, 'orders', log);
        assert.ifError(err);
        assert.strictEqual(storedRecord('orders'), undefined);
    });

    it('should delete a destination whose name a global destination uses', async () => {
        // Create refuses such a name. Delete still removes a record that holds it.
        await seed({ ...storedSaslSsl, Name: 'target1', Arn: `arn:scality:bucketnotif::${accountId}:target1` }, log);
        const saved = config.bucketNotificationDestinations;
        config.bucketNotificationDestinations = [{ resource: 'target1', type: 'dummy', host: 'localhost:6000' }];
        try {
            const [err] = await remove(accountAuthInfo, 'target1', log);
            assert.ifError(err);
            assert.strictEqual(storedRecord('target1'), undefined);
        } finally {
            config.bucketNotificationDestinations = saved;
        }
    });

    it('should answer NoSuchDestination to an unknown name, and delete nothing', async () => {
        const del = sinon.spy(recordStore, 'deleteDestination');
        const [err] = await remove(accountAuthInfo, 'unknown', log);
        assert.strictEqual(err?.message, 'NoSuchDestination');
        assert.strictEqual(err.code, 404);
        assert.strictEqual(del.called, false);
    });

    it('should answer NoSuchDestination to a name deleted already', async () => {
        assert.ifError((await remove(accountAuthInfo, 'orders', log))[0]);
        const [err] = await remove(accountAuthInfo, 'orders', log);
        assert.strictEqual(err?.message, 'NoSuchDestination');
    });

    it('should answer NoSuchDestination to the name of another account, and leave the record unchanged', async () => {
        const del = sinon.spy(recordStore, 'deleteDestination');
        const [err] = await remove(otherAuthInfo, 'orders', log);
        assert.strictEqual(err?.message, 'NoSuchDestination');
        assert.strictEqual(del.called, false);
        assert.deepStrictEqual(storedRecord('orders'), storedSaslSsl);
    });

    it('should answer NoSuchDestination when the record goes between the read and the delete', async () => {
        // Another Delete of the name may land between the two calls.
        sinon.stub(recordStore, 'deleteDestination').callsArgWith(3, errors.NoSuchDestination);
        const [err] = await remove(accountAuthInfo, 'orders', log);
        assert.strictEqual(err?.message, 'NoSuchDestination');
    });

    ['bad/name', 'a'.repeat(models.NotificationDestination.nameMaxLength + 1), '*', ''].forEach(name => {
        it(`should answer InvalidDestination to the name '${name.slice(0, 20)}', and read nothing`, async () => {
            const read = sinon.spy(recordStore, 'getDestination');
            const del = sinon.spy(recordStore, 'deleteDestination');
            const [err] = await remove(accountAuthInfo, name, log);
            assert.strictEqual(err?.message, 'InvalidDestination');
            assert.strictEqual(err.code, 400);
            assert.strictEqual(read.called, false);
            assert.strictEqual(del.called, false);
        });
    });

    it('should pass a record store read error through, and delete nothing', async () => {
        sinon.stub(recordStore, 'getDestination').callsArgWith(3, errors.InternalError);
        const del = sinon.spy(recordStore, 'deleteDestination');
        const [err] = await remove(accountAuthInfo, 'orders', log);
        assert.strictEqual(err?.message, 'InternalError');
        assert.strictEqual(del.called, false);
        assert.deepStrictEqual(storedRecord('orders'), storedSaslSsl);
    });

    it('should pass a record store delete error through', async () => {
        sinon.stub(recordStore, 'deleteDestination').callsArgWith(3, errors.InternalError);
        const [err] = await remove(accountAuthInfo, 'orders', log);
        assert.strictEqual(err?.message, 'InternalError');
    });

    it('should never log the record', async () => {
        const spyLog = new DummyRequestLogger();
        const levels = ['trace', 'debug', 'info', 'warn', 'error'];
        levels.forEach(level => sinon.spy(spyLog, level));
        const [otherErr] = await remove(otherAuthInfo, 'orders', spyLog);
        assert.strictEqual(otherErr?.message, 'NoSuchDestination');
        const [deleteErr] = await remove(accountAuthInfo, 'orders', spyLog);
        assert.ifError(deleteErr);
        const [againErr] = await remove(accountAuthInfo, 'orders', spyLog);
        assert.strictEqual(againErr?.message, 'NoSuchDestination');
        const logged = JSON.stringify(levels.flatMap(level => spyLog[level].args));
        ['sentinel-user-7a3f', 'sentinel-pass-7a3f', 'sentinel-ca-7a3f', 'broker', 'events'].forEach(value =>
            assert.doesNotMatch(logged, new RegExp(value)),
        );
        assert.match(logged, /orders/);
        assert.match(logged, new RegExp(accountId));
    });
});
