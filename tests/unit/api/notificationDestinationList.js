const assert = require('assert');
const sinon = require('sinon');
const { errors } = require('@scality/arsenal');

const notificationDestinationCreate = require('../../../lib/api/notificationDestinationCreate');
const notificationDestinationList = require('../../../lib/api/notificationDestinationList');
const recordStore = require('../../../lib/api/apiUtils/notificationDestination/recordStore');
const { listingHardLimit } = require('../../../constants');
const { DummyRequestLogger } = require('../helpers');
const {
    accountId,
    accountAuthInfo,
    otherAuthInfo,
    userAuthInfo,
    makeRequest,
    listRequest,
    call,
    useMemRecordStore,
} = require('../utils/notificationDestinationHelpers');

// the account of accessKey2 in makeAuthInfo
const otherAccountId = '234567890987';

const secrets = ['sentinel-user-5c1a', 'sentinel-pass-5c1a', 'sentinel-ca-5c1a'];

const saslSslBody =
    '<NotificationDestination>' +
    '<Type>kafka</Type>' +
    '<Connection>' +
    '<Brokers><Broker>broker1.example.com:9093</Broker></Brokers>' +
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

const list = (authInfo, params, log) => call(notificationDestinationList, authInfo, listRequest(params), log);

async function createAll(authInfo, names, log) {
    for (const name of names) {
        const [err] = await call(notificationDestinationCreate, authInfo, makeRequest('PUT', name, saslSslBody), log);
        assert.ifError(err);
    }
}

// The text of each element of that name in an XML body.
function elementValues(body, element) {
    return [...body.matchAll(new RegExp(`<${element}>([^<]*)</${element}>`, 'g'))].map(([, value]) => value);
}

const listedNames = xml => [...xml.matchAll(/<NotificationDestination><Name>([^<]*)<\/Name>/g)].map(([, name]) => name);

describe('notificationDestinationList', () => {
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

    it('should answer IsTruncated false and no entry to an account with no destination', async () => {
        const args = await list(accountAuthInfo, {}, log);
        assert.strictEqual(args.length, 2, 'List answers callback(err, xml)');
        const [err, xml] = args;
        assert.ifError(err);
        assert.strictEqual(
            xml,
            '<?xml version="1.0" encoding="UTF-8"?>' +
                '<ListNotificationDestinationsResult><IsTruncated>false</IsTruncated>' +
                '</ListNotificationDestinationsResult>',
        );
    });

    it('should answer every destination in name order, as Get answers each, with no secret', async () => {
        await createAll(accountAuthInfo, ['orders', 'audit', 'billing'], log);
        const [err, xml] = await list(accountAuthInfo, {}, log);
        assert.ifError(err);
        assert.match(
            xml,
            /^<\?xml version="1\.0" encoding="UTF-8"\?><ListNotificationDestinationsResult><IsTruncated>false<\/IsTruncated><NotificationDestination>.*<\/NotificationDestination><\/ListNotificationDestinationsResult>$/,
        );
        assert.doesNotMatch(xml, /xmlns|<NextMarker>/);
        assert.deepStrictEqual(listedNames(xml), ['audit', 'billing', 'orders']);
        assert.deepStrictEqual(elementValues(xml, 'AccountId'), [accountId, accountId, accountId]);
        assert.match(
            xml,
            new RegExp(
                `<NotificationDestination><Name>audit</Name><Arn>arn:scality:bucketnotif::${accountId}:audit</Arn>` +
                    `<AccountId>${accountId}</AccountId><Type>kafka</Type><Connection>` +
                    '<Brokers><Broker>broker1.example.com:9093</Broker></Brokers><Topic>bucket-events</Topic>' +
                    '<RequiredAcks>1</RequiredAcks><CompressionType>zstd</CompressionType></Connection>' +
                    '<Auth><Type>sasl_ssl</Type></Auth><CreatedAt>[^<]+</CreatedAt><UpdatedAt>[^<]+</UpdatedAt>' +
                    '</NotificationDestination>',
            ),
        );
        secrets.forEach(secret => assert.doesNotMatch(xml, new RegExp(secret)));
        assert.doesNotMatch(xml, /<Sasl>|<Tls>|<Username>|<Password>/);
    });

    it('should list the destinations of the account of an IAM user', async () => {
        await createAll(accountAuthInfo, ['orders'], log);
        const [err, xml] = await list(userAuthInfo, {}, log);
        assert.ifError(err);
        assert.deepStrictEqual(listedNames(xml), ['orders']);
    });

    it('should never list the destinations of another account', async () => {
        await createAll(otherAuthInfo, ['other-a', 'other-b'], log);
        await createAll(accountAuthInfo, ['mine'], log);
        const [err, xml] = await list(accountAuthInfo, {}, log);
        assert.ifError(err);
        assert.deepStrictEqual(listedNames(xml), ['mine']);
        const [otherErr, otherXml] = await list(otherAuthInfo, {}, log);
        assert.ifError(otherErr);
        assert.deepStrictEqual(listedNames(otherXml), ['other-a', 'other-b']);
        assert.deepStrictEqual(elementValues(otherXml, 'AccountId'), [otherAccountId, otherAccountId]);
    });

    it('should page through three names with max-items 2, NextMarker then marker', async () => {
        await createAll(accountAuthInfo, ['c-dest', 'a-dest', 'b-dest'], log);
        const [err, xml] = await list(accountAuthInfo, { 'max-items': '2' }, log);
        assert.ifError(err);
        assert.match(
            xml,
            /<ListNotificationDestinationsResult><IsTruncated>true<\/IsTruncated><NextMarker>b-dest<\/NextMarker><NotificationDestination>/,
        );
        assert.deepStrictEqual(listedNames(xml), ['a-dest', 'b-dest']);
        const [nextMarker] = elementValues(xml, 'NextMarker');

        const [nextErr, nextXml] = await list(accountAuthInfo, { 'max-items': '2', 'marker': nextMarker }, log);
        assert.ifError(nextErr);
        assert.match(
            nextXml,
            /<ListNotificationDestinationsResult><IsTruncated>false<\/IsTruncated><NotificationDestination>/,
        );
        assert.doesNotMatch(nextXml, /<NextMarker>/);
        assert.deepStrictEqual(listedNames(nextXml), ['c-dest']);
    });

    it('should answer IsTruncated false and no NextMarker to a page that the names fill', async () => {
        await createAll(accountAuthInfo, ['b-dest', 'a-dest'], log);
        const [err, xml] = await list(accountAuthInfo, { 'max-items': '2' }, log);
        assert.ifError(err);
        assert.deepStrictEqual(listedNames(xml), ['a-dest', 'b-dest']);
        assert.deepStrictEqual(elementValues(xml, 'IsTruncated'), ['false']);
        assert.doesNotMatch(xml, /<NextMarker>/);
    });

    it('should list the names after a marker that is not a destination name', async () => {
        await createAll(accountAuthInfo, ['a-dest', 'b-dest', 'c-dest'], log);
        const [err, xml] = await list(accountAuthInfo, { marker: 'a-dest0' }, log);
        assert.ifError(err);
        assert.deepStrictEqual(listedNames(xml), ['b-dest', 'c-dest']);
    });

    it('should escape NextMarker', async () => {
        // No valid name needs escaping, so the store gives one that does.
        const record = {
            Name: 'a',
            Arn: `arn:scality:bucketnotif::${accountId}:a`,
            AccountId: accountId,
            Type: 'kafka',
            Connection: { Brokers: ['b:1'], Topic: 't', RequiredAcks: 1, CompressionType: 'none' },
            Auth: { Type: 'none' },
            CreatedAt: '2026-11-10T09:15:00Z',
            UpdatedAt: '2026-11-10T09:15:00Z',
        };
        sinon
            .stub(recordStore, 'listDestinations')
            .callsArgWith(3, null, { destinations: [{ name: 'a', record }], isTruncated: true, nextMarker: 'a&<b>' });
        const [err, xml] = await list(accountAuthInfo, { 'max-items': '1' }, log);
        assert.ifError(err);
        assert.match(xml, /<IsTruncated>true<\/IsTruncated><NextMarker>a&amp;&lt;b&gt;<\/NextMarker>/);
    });

    it('should ask the store for 100 names when max-items is absent', async () => {
        const listing = sinon.spy(recordStore, 'listDestinations');
        const [err] = await list(accountAuthInfo, {}, log);
        assert.ifError(err);
        assert.strictEqual(listing.calledOnce, true);
        assert.strictEqual(listing.firstCall.args[0], accountId);
        assert.deepStrictEqual(listing.firstCall.args[1], { maxKeys: 100 });
    });

    it('should answer 100 entries and NextMarker when max-items is absent', async () => {
        const destinations = Array.from({ length: 101 }, (_, i) => `dest-${String(i).padStart(3, '0')}`);
        await createAll(accountAuthInfo, destinations, log);
        const [err, xml] = await list(accountAuthInfo, {}, log);
        assert.ifError(err);
        assert.deepStrictEqual(listedNames(xml), destinations.slice(0, 100));
        assert.deepStrictEqual(elementValues(xml, 'IsTruncated'), ['true']);
        assert.deepStrictEqual(elementValues(xml, 'NextMarker'), ['dest-099']);
    });

    [
        ['1', 1],
        ['007', 7],
        [String(listingHardLimit), listingHardLimit],
    ].forEach(([maxItems, maxKeys]) => {
        it(`should ask the store for ${maxKeys} names with max-items ${maxItems}`, async () => {
            const listing = sinon.spy(recordStore, 'listDestinations');
            const [err] = await list(accountAuthInfo, { 'max-items': maxItems, 'marker': 'm' }, log);
            assert.ifError(err);
            assert.deepStrictEqual(listing.firstCall.args[1], { maxKeys, marker: 'm' });
        });
    });

    [
        ['0', '0'],
        ['above the listing limit', String(listingHardLimit + 1)],
        ['abc', 'abc'],
        ['2.5', '2.5'],
        ['empty', ''],
        ['a leading plus', '+5'],
        ['a minus', '-1'],
        ['a space', ' 5'],
        ['an exponent', '1e2'],
    ].forEach(([what, maxItems]) => {
        it(`should answer InvalidArgument to max-items ${what}, with no listing`, async () => {
            const listing = sinon.spy(recordStore, 'listDestinations');
            const args = await list(accountAuthInfo, { 'max-items': maxItems }, log);
            assert.strictEqual(args.length, 2);
            const [err, xml] = args;
            assert.strictEqual(err?.message, 'InvalidArgument');
            assert.strictEqual(err.code, 400);
            assert.strictEqual(err.description, `max-items must be a whole number from 1 to ${listingHardLimit}.`);
            assert.strictEqual(xml, null);
            assert.strictEqual(listing.called, false);
        });
    });

    it('should answer InvalidArgument to a repeated max-items, with no listing', async () => {
        // Each value is valid alone, so the answer names the repeat.
        const listing = sinon.spy(recordStore, 'listDestinations');
        const [err, xml] = await list(accountAuthInfo, { 'max-items': ['2', '3'] }, log);
        assert.strictEqual(err?.message, 'InvalidArgument');
        assert.strictEqual(err.description, 'max-items must be given once.');
        assert.strictEqual(xml, null);
        assert.strictEqual(listing.called, false);
    });

    it('should answer InvalidArgument to a repeated marker, with no listing', async () => {
        const listing = sinon.spy(recordStore, 'listDestinations');
        const [err, xml] = await list(accountAuthInfo, { marker: ['a', 'b'] }, log);
        assert.strictEqual(err?.message, 'InvalidArgument');
        assert.strictEqual(err.description, 'marker must be given once.');
        assert.strictEqual(xml, null);
        assert.strictEqual(listing.called, false);
    });

    it('should check a repeated marker before the max-items value', async () => {
        const [err] = await list(accountAuthInfo, { 'max-items': 'abc', 'marker': ['a', 'b'] }, log);
        assert.strictEqual(err?.message, 'InvalidArgument');
        assert.strictEqual(err.description, 'marker must be given once.');
    });

    it('should pass a record store listing error through', async () => {
        sinon.stub(recordStore, 'listDestinations').callsArgWith(3, errors.InternalError);
        const [err, xml] = await list(accountAuthInfo, {}, log);
        assert.strictEqual(err?.message, 'InternalError');
        assert.strictEqual(xml, null);
    });

    it('should answer InternalError when a listed record cannot be answered', async () => {
        const record = {
            Name: 'orders',
            Arn: `arn:scality:bucketnotif::${accountId}:orders`,
            AccountId: accountId,
            Type: 'kafka',
            Auth: { Type: 'none' },
            CreatedAt: '2026-11-10T09:15:00Z',
            UpdatedAt: '2026-11-10T09:15:00Z',
        };
        sinon
            .stub(recordStore, 'listDestinations')
            .callsArgWith(3, null, { destinations: [{ name: 'orders', record }], isTruncated: false });
        const [err, xml] = await list(accountAuthInfo, {}, log);
        assert.strictEqual(err?.message, 'InternalError');
        assert.strictEqual(xml, null);
    });

    it('should log the account and the counts, never a name or a record', async () => {
        await createAll(accountAuthInfo, ['orders', 'audit'], log);
        const spyLog = new DummyRequestLogger();
        const levels = ['trace', 'debug', 'info', 'warn', 'error'];
        levels.forEach(level => sinon.spy(spyLog, level));
        const [err] = await list(accountAuthInfo, { 'max-items': '1', 'marker': 'a-marker' }, spyLog);
        assert.ifError(err);
        const logged = JSON.stringify(levels.flatMap(level => spyLog[level].args));
        [...secrets, 'broker1', 'bucket-events', 'orders', 'audit', 'a-marker'].forEach(value => {
            assert.doesNotMatch(logged, new RegExp(value));
        });
        const [, listedFields] = spyLog.debug.args.find(([message]) => message === 'destinations listed');
        assert.deepStrictEqual(listedFields, {
            method: 'notificationDestinationList',
            accountId,
            maxItems: 1,
            count: 1,
            isTruncated: true,
        });
    });
});
