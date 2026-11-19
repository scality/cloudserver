const assert = require('assert');
const sinon = require('sinon');
const { errors, models } = require('@scality/arsenal');

const notificationDestinationUpdate = require('../../../lib/api/notificationDestinationUpdate');
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

// A stored record, written long ago, that each test updates.
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
        Sasl: { Mechanism: 'SCRAM-SHA-512', Username: 'old-user-4c1e', Password: 'old-pass-4c1e' },
        Tls: { CaCertificate: 'old-ca-4c1e' },
    },
    CreatedAt: '2020-01-01T00:00:00Z',
    UpdatedAt: '2020-01-01T00:00:00Z',
};

// The new body: other brokers and topic, mtls, and no RequiredAcks or
// CompressionType, so that the defaults apply.
const mtlsBody =
    '<NotificationDestination>' +
    '<Type>kafka</Type>' +
    '<Connection>' +
    '<Brokers><Broker>broker2.example.com:9094</Broker><Broker>broker3.example.com:9094</Broker></Brokers>' +
    '<Topic>new-events</Topic>' +
    '</Connection>' +
    '<Auth>' +
    '<Type>mtls</Type>' +
    '<Tls><ClientCertificate>sentinel-cert-5e2d</ClientCertificate><ClientKey>sentinel-key-5e2d</ClientKey></Tls>' +
    '</Auth>' +
    '</NotificationDestination>';

// The record that Update writes from mtlsBody, with no UpdatedAt.
const updatedRecord = {
    modelVersion: 1,
    Name: 'orders',
    AccountId: accountId,
    Arn: `arn:scality:bucketnotif::${accountId}:orders`,
    Type: 'kafka',
    Connection: {
        Brokers: ['broker2.example.com:9094', 'broker3.example.com:9094'],
        Topic: 'new-events',
        RequiredAcks: -1,
        CompressionType: 'none',
    },
    Auth: {
        Type: 'mtls',
        Tls: { ClientCertificate: 'sentinel-cert-5e2d', ClientKey: 'sentinel-key-5e2d' },
    },
    CreatedAt: '2020-01-01T00:00:00Z',
};

const update = (authInfo, name, post, log) =>
    call(notificationDestinationUpdate, authInfo, makeRequest('POST', name, post), log);

describe('notificationDestinationUpdate', () => {
    let log;
    let restoreRecordStore;
    let savedGlobalDestinations;

    beforeEach(async () => {
        restoreRecordStore = useMemRecordStore();
        log = new DummyRequestLogger();
        savedGlobalDestinations = config.bucketNotificationDestinations;
        config.bucketNotificationDestinations = [{ resource: 'target1', type: 'dummy', host: 'localhost:6000' }];
        await seed(storedSaslSsl, log);
    });

    afterEach(() => {
        restoreRecordStore();
        config.bucketNotificationDestinations = savedGlobalDestinations;
        sinon.restore();
    });

    it('should replace the record, keep CreatedAt, and answer with no body', async () => {
        const before = Date.now();
        const args = await update(accountAuthInfo, 'orders', mtlsBody, log);
        assert.strictEqual(args.length, 1, 'Update answers callback(err)');
        assert.ifError(args[0]);

        const { UpdatedAt, ...rest } = storedRecord('orders');
        assert.deepStrictEqual(rest, updatedRecord);
        assert.match(UpdatedAt, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/);
        assert(Date.parse(UpdatedAt) >= before);
    });

    it('should rewrite a version 1 record with version 1, and drop any other stored field', async () => {
        await seed({ ...storedSaslSsl, modelVersion: 1, Extra: 'stale' }, log);
        const [err] = await update(accountAuthInfo, 'orders', mtlsBody, log);
        assert.ifError(err);
        const record = storedRecord('orders');
        assert.strictEqual(record.modelVersion, 1);
        assert.strictEqual(record.Extra, undefined);
    });

    it('should rewrite a record with no modelVersion as version 1, and keep CreatedAt', async () => {
        const unversioned = { ...storedSaslSsl };
        delete unversioned.modelVersion;
        await seed(unversioned, log);
        const [err] = await update(accountAuthInfo, 'orders', mtlsBody, log);
        assert.ifError(err);
        const { UpdatedAt, ...rest } = storedRecord('orders');
        assert.deepStrictEqual(rest, updatedRecord);
        assert.notStrictEqual(UpdatedAt, storedSaslSsl.UpdatedAt);
    });

    it('should refuse a record written by a newer server, and leave it unchanged', async () => {
        const newer = { ...storedSaslSsl, modelVersion: 2 };
        await seed(newer, log);
        const put = sinon.spy(recordStore, 'putDestination');
        const [err] = await update(accountAuthInfo, 'orders', mtlsBody, log);
        assert.strictEqual(err?.message, 'ServiceUnavailable');
        assert.strictEqual(err.code, 503);
        assert.match(err.description, /written by a newer server \(record version 2, this server knows 1\)/);
        assert.strictEqual(put.called, false);
        assert.deepStrictEqual(storedRecord('orders'), newer);
    });

    it('should update the destination of the account of an IAM user, never its user ID', async () => {
        const [err] = await update(userAuthInfo, 'orders', mtlsBody, log);
        assert.ifError(err);
        assert.strictEqual(storedRecord('orders').Connection.Topic, 'new-events');
    });

    it('should answer NoSuchDestination to an unknown name, and write nothing', async () => {
        const put = sinon.spy(recordStore, 'putDestination');
        const [err] = await update(accountAuthInfo, 'unknown', mtlsBody, log);
        assert.strictEqual(err?.message, 'NoSuchDestination');
        assert.strictEqual(err.code, 404);
        assert.strictEqual(put.called, false);
        assert.strictEqual(storedRecord('unknown'), undefined);
    });

    it('should answer NoSuchDestination to the name of another account', async () => {
        const [err] = await update(otherAuthInfo, 'orders', mtlsBody, log);
        assert.strictEqual(err?.message, 'NoSuchDestination');
        assert.strictEqual(storedRecord('orders').Connection.Topic, 'bucket-events');
    });

    it('should update a destination whose name a global destination uses', async () => {
        // Create refuses such a name. Update still updates a record that holds it.
        await seed({ ...storedSaslSsl, Name: 'target1', Arn: `arn:scality:bucketnotif::${accountId}:target1` }, log);
        const [err] = await update(accountAuthInfo, 'target1', mtlsBody, log);
        assert.ifError(err);
        assert.strictEqual(storedRecord('target1').Connection.Topic, 'new-events');
    });

    ['bad/name', 'a'.repeat(models.NotificationDestination.nameMaxLength + 1), '*'].forEach(name => {
        it(`should answer InvalidDestination to the name '${name.slice(0, 20)}'`, async () => {
            const get = sinon.spy(recordStore, 'getDestination');
            const [err] = await update(accountAuthInfo, name, mtlsBody, log);
            assert.strictEqual(err?.message, 'InvalidDestination');
            assert.strictEqual(err.code, 400);
            assert.strictEqual(get.called, false);
        });
    });

    it('should check the name before the body', async () => {
        const [err] = await update(accountAuthInfo, 'bad/name', '', log);
        assert.strictEqual(err?.message, 'InvalidDestination');
    });

    [undefined, '<NotificationDestination>'].forEach(post => {
        it(`should answer MalformedXML to the body ${JSON.stringify(post)}, and leave the record unchanged`, async () => {
            const [err] = await update(accountAuthInfo, 'orders', post, log);
            assert.strictEqual(err?.message, 'MalformedXML');
            assert.deepStrictEqual(storedRecord('orders'), storedSaslSsl);
        });
    });

    [
        [
            'a broker with port 0',
            ['broker2.example.com:9094', 'broker2.example.com:0'],
            /^Connection\.Brokers\.Broker /,
        ],
        [
            'the GSSAPI mechanism',
            [
                /<Auth>.*<\/Auth>/,
                '<Auth><Type>sasl_ssl</Type><Sasl><Mechanism>GSSAPI</Mechanism>' +
                    '<Username>u</Username><Password>p</Password></Sasl></Auth>',
            ],
            /Sasl\.Mechanism/,
        ],
        ['mtls with no client key', ['<ClientKey>sentinel-key-5e2d</ClientKey>', ''], /ClientKey/],
    ].forEach(([what, [from, to], description]) => {
        it(`should answer InvalidDestination to ${what}, and leave the record unchanged`, async () => {
            const body = mtlsBody.replace(from, to);
            assert.notStrictEqual(body, mtlsBody);
            const [err] = await update(accountAuthInfo, 'orders', body, log);
            assert.strictEqual(err?.message, 'InvalidDestination');
            assert.match(err.description, description);
            assert.deepStrictEqual(storedRecord('orders'), storedSaslSsl);
        });
    });

    it('should check the body before reading the record', async () => {
        const get = sinon.spy(recordStore, 'getDestination');
        const [err] = await update(accountAuthInfo, 'unknown', '<NotificationDestination>', log);
        assert.strictEqual(err?.message, 'MalformedXML');
        assert.strictEqual(get.called, false);
    });

    it('should pass a record store read error through, and write nothing', async () => {
        sinon.stub(recordStore, 'getDestination').callsArgWith(3, errors.InternalError);
        const put = sinon.spy(recordStore, 'putDestination');
        const [err] = await update(accountAuthInfo, 'orders', mtlsBody, log);
        assert.strictEqual(err?.message, 'InternalError');
        assert.strictEqual(put.called, false);
    });

    it('should pass a record store write error through', async () => {
        sinon.stub(recordStore, 'putDestination').callsArgWith(4, errors.InternalError);
        const [err] = await update(accountAuthInfo, 'orders', mtlsBody, log);
        assert.strictEqual(err?.message, 'InternalError');
    });

    it('should never log the record', async () => {
        const spyLog = new DummyRequestLogger();
        const levels = ['trace', 'debug', 'info', 'warn', 'error'];
        levels.forEach(level => sinon.spy(spyLog, level));
        await update(accountAuthInfo, 'orders', mtlsBody, spyLog);
        await update(accountAuthInfo, 'unknown', mtlsBody, spyLog);
        await update(accountAuthInfo, 'orders', mtlsBody.replace('broker2.example.com:9094', 'x:0'), spyLog);
        await seed({ ...storedSaslSsl, modelVersion: 2 }, log);
        const [newerErr] = await update(accountAuthInfo, 'orders', mtlsBody, spyLog);
        assert.match(newerErr?.description ?? '', /newer server/);
        const logged = JSON.stringify(levels.flatMap(level => spyLog[level].args));
        [
            'sentinel-cert-5e2d',
            'sentinel-key-5e2d',
            'old-pass-4c1e',
            'old-user-4c1e',
            'old-ca-4c1e',
            'broker',
            'events',
        ].forEach(value => assert.doesNotMatch(logged, new RegExp(value)));
        assert.match(logged, /orders/);
        assert.match(logged, new RegExp(accountId));
    });
});
