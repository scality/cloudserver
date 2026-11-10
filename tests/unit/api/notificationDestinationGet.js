const assert = require('assert');
const sinon = require('sinon');
const { errors, models } = require('@scality/arsenal');
const { config } = require('../../../lib/Config');

const notificationDestinationCreate = require('../../../lib/api/notificationDestinationCreate');
const notificationDestinationGet = require('../../../lib/api/notificationDestinationGet');
const recordStore = require('../../../lib/api/apiUtils/notificationDestination/recordStore');
const { DummyRequestLogger } = require('../helpers');
const {
    accountId,
    accountAuthInfo,
    otherAuthInfo,
    userAuthInfo,
    makeRequest,
    call,
    useMemRecordStore,
} = require('../utils/notificationDestinationHelpers');

const secrets = ['sentinel-user-9d2b', 'sentinel-pass-9d2b', 'sentinel-ca-9d2b'];

const saslSslBody =
    '<NotificationDestination>' +
    '<Type>kafka</Type>' +
    '<Connection>' +
    '<Brokers><Broker>broker1.example.com:9093</Broker><Broker>broker2.example.com:9093</Broker></Brokers>' +
    '<Topic>bucket-events</Topic>' +
    '<RequiredAcks>1</RequiredAcks><CompressionType>zstd</CompressionType>' +
    '</Connection>' +
    '<Auth>' +
    '<Type>sasl_ssl</Type>' +
    `<Sasl><Mechanism>SCRAM-SHA-512</Mechanism><Username>${secrets[0]}</Username>` +
    `<Password>${secrets[1]}</Password></Sasl>` +
    `<Tls><CaCertificate>${secrets[2]}</CaCertificate></Tls>` +
    '</Auth>' +
    '</NotificationDestination>';

const create = (authInfo, name, log) =>
    call(notificationDestinationCreate, authInfo, makeRequest('PUT', name, saslSslBody), log);
const get = (authInfo, name, log) => call(notificationDestinationGet, authInfo, makeRequest('GET', name), log);

describe('notificationDestinationGet', () => {
    let log;
    let restoreRecordStore;

    beforeEach(() => {
        restoreRecordStore = useMemRecordStore();
        log = new DummyRequestLogger();
    });

    afterEach(() => {
        restoreRecordStore();
        sinon.restore();
    });

    it('should answer the created destination as XML, with no secret', async () => {
        const [createErr] = await create(accountAuthInfo, 'orders', log);
        assert.ifError(createErr);
        const args = await get(accountAuthInfo, 'orders', log);
        assert.strictEqual(args.length, 2, 'Get answers callback(err, xml)');
        const [err, xml] = args;
        assert.ifError(err);
        const match = xml.match(
            /^<\?xml version="1\.0" encoding="UTF-8"\?><NotificationDestination><Name>orders<\/Name>.*<CreatedAt>(.*)<\/CreatedAt><UpdatedAt>(.*)<\/UpdatedAt><\/NotificationDestination>$/,
        );
        assert(match, xml);
        assert.match(xml, new RegExp(`<Arn>arn:scality:bucketnotif::${accountId}:orders</Arn>`));
        assert.match(xml, new RegExp(`<AccountId>${accountId}</AccountId>`));
        assert.match(
            xml,
            new RegExp(
                '<Type>kafka</Type><Connection><Brokers><Broker>broker1.example.com:9093</Broker>' +
                    '<Broker>broker2.example.com:9093</Broker></Brokers><Topic>bucket-events</Topic>' +
                    '<RequiredAcks>1</RequiredAcks><CompressionType>zstd</CompressionType></Connection>' +
                    '<Auth><Type>sasl_ssl</Type></Auth>',
            ),
        );
        const [, createdAt, updatedAt] = match;
        // Both dates are ISO 8601 in UTC, with milliseconds.
        assert.match(createdAt, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/);
        // A record that was never updated has its creation date as its
        // update date.
        assert.strictEqual(updatedAt, createdAt);
        secrets.forEach(secret => assert.doesNotMatch(xml, new RegExp(secret)));
    });

    it('should read the destination of the account of an IAM user', async () => {
        const [createErr] = await create(accountAuthInfo, 'orders', log);
        assert.ifError(createErr);
        const [err, xml] = await get(userAuthInfo, 'orders', log);
        assert.ifError(err);
        assert.match(xml, new RegExp(`<AccountId>${accountId}</AccountId>`));
    });

    it('should answer NoSuchDestination to an unknown name', async () => {
        const args = await get(accountAuthInfo, 'unknown', log);
        assert.strictEqual(args.length, 2);
        const [err, xml] = args;
        assert.strictEqual(err?.message, 'NoSuchDestination');
        assert.strictEqual(err.code, 404);
        assert.strictEqual(xml, null);
    });

    it('should answer NoSuchDestination to the name of a global destination', async () => {
        const saved = config.bucketNotificationDestinations;
        config.bucketNotificationDestinations = [{ resource: 'target1', type: 'dummy', host: 'localhost:6000' }];
        try {
            const [err, xml] = await get(accountAuthInfo, 'target1', log);
            assert.strictEqual(err?.message, 'NoSuchDestination');
            assert.strictEqual(xml, null);
        } finally {
            config.bucketNotificationDestinations = saved;
        }
    });

    it('should answer NoSuchDestination to the name of another account', async () => {
        const [createErr] = await create(otherAuthInfo, 'orders', log);
        assert.ifError(createErr);
        const [err] = await get(accountAuthInfo, 'orders', log);
        assert.strictEqual(err?.message, 'NoSuchDestination');
    });

    ['bad/name', 'a'.repeat(models.NotificationDestination.nameMaxLength + 1), 'na:me', '*'].forEach(name => {
        it(`should answer InvalidDestination to the name '${name.slice(0, 20)}' with no read`, async () => {
            const read = sinon.spy(recordStore, 'getDestination');
            const [err, xml] = await get(accountAuthInfo, name, log);
            assert.strictEqual(err?.message, 'InvalidDestination');
            assert.strictEqual(xml, null);
            assert.strictEqual(read.called, false);
        });
    });

    it('should pass a record store read error through', async () => {
        sinon.stub(recordStore, 'getDestination').callsArgWith(3, errors.InternalError);
        const [err, xml] = await get(accountAuthInfo, 'orders', log);
        assert.strictEqual(err?.message, 'InternalError');
        assert.strictEqual(xml, null);
    });

    // A whole record, as the store would give it, that a test may break.
    const wholeRecord = () => ({
        Name: 'orders',
        Arn: `arn:scality:bucketnotif::${accountId}:orders`,
        AccountId: accountId,
        Type: 'kafka',
        Connection: {
            Brokers: ['broker1.example.com:9093'],
            Topic: 't',
            RequiredAcks: -1,
            CompressionType: 'none',
        },
        Auth: { Type: 'none' },
        CreatedAt: '2026-11-10T09:15:00Z',
        UpdatedAt: '2026-11-10T09:15:00Z',
    });

    it('should answer a whole stored record', async () => {
        sinon.stub(recordStore, 'getDestination').callsArgWith(3, null, wholeRecord());
        const [err, xml] = await get(accountAuthInfo, 'orders', log);
        assert.ifError(err);
        assert.match(xml, /<Name>orders<\/Name>/);
        assert.match(xml, /<Brokers><Broker>broker1\.example\.com:9093<\/Broker><\/Brokers>/);
        assert.match(xml, /<Topic>t<\/Topic><RequiredAcks>-1<\/RequiredAcks><CompressionType>none<\/CompressionType>/);
        assert.match(xml, /<Auth><Type>none<\/Type><\/Auth>/);
        assert.match(xml, /<UpdatedAt>2026-11-10T09:15:00Z<\/UpdatedAt>/);
    });

    ['UpdatedAt', 'Connection', 'Auth'].forEach(field => {
        it(`should answer InternalError to a record with no ${field}`, async () => {
            const record = wholeRecord();
            delete record[field];
            sinon.stub(recordStore, 'getDestination').callsArgWith(3, null, record);
            const [err, xml] = await get(accountAuthInfo, 'orders', log);
            assert.strictEqual(err?.message, 'InternalError');
            assert.strictEqual(xml, null);
        });
    });

    it('should not log a record that it cannot answer', async () => {
        const record = wholeRecord();
        delete record.UpdatedAt;
        record.Auth = {
            Type: 'sasl_ssl',
            Sasl: { Mechanism: 'SCRAM-SHA-512', Username: secrets[0], Password: secrets[1] },
            Tls: { CaCertificate: secrets[2] },
        };
        sinon.stub(recordStore, 'getDestination').callsArgWith(3, null, record);
        const spyLog = new DummyRequestLogger();
        ['trace', 'debug', 'info', 'warn', 'error'].forEach(level => sinon.spy(spyLog, level));
        const [err] = await get(accountAuthInfo, 'orders', spyLog);
        assert.strictEqual(err?.message, 'InternalError');
        assert(spyLog.error.called, 'Get logs the failure as an error');
        const logged = JSON.stringify(['trace', 'debug', 'info', 'warn', 'error'].flatMap(level => spyLog[level].args));
        [...secrets, 'broker1'].forEach(value => assert.doesNotMatch(logged, new RegExp(value)));
    });

    it('should never log the record', async () => {
        const [createErr] = await create(accountAuthInfo, 'orders', log);
        assert.ifError(createErr);
        const spyLog = new DummyRequestLogger();
        ['trace', 'debug', 'info', 'warn', 'error'].forEach(level => sinon.spy(spyLog, level));
        const [getErr] = await get(accountAuthInfo, 'orders', spyLog);
        assert.ifError(getErr);
        const [unknownErr] = await get(accountAuthInfo, 'unknown', spyLog);
        assert.strictEqual(unknownErr?.message, 'NoSuchDestination');
        const logged = JSON.stringify(['trace', 'debug', 'info', 'warn', 'error'].flatMap(level => spyLog[level].args));
        [...secrets, 'broker1', 'bucket-events'].forEach(value => {
            assert.doesNotMatch(logged, new RegExp(value));
        });
        assert.match(logged, /orders/);
        assert.match(logged, new RegExp(accountId));
    });
});
