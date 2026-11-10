const assert = require('assert');

const { destinationXml } = require('../../../../../lib/api/apiUtils/notificationDestination/xml');

const accountId = '123456789012';

// Strings that only a secret field holds, so that finding one in the output
// means a secret leaked.
const secrets = {
    username: 'sentinel-user-5c1e',
    password: 'sentinel-pass-5c1e',
    clientCertificate: 'sentinel-cert-5c1e',
    clientKey: 'sentinel-key-5c1e',
    clientKeyPassword: 'sentinel-keypass-5c1e',
    caCertificate: 'sentinel-ca-5c1e',
};

const auths = {
    none: { Type: 'none' },
    // eslint-disable-next-line camelcase
    sasl_plaintext: {
        Type: 'sasl_plaintext',
        Sasl: { Mechanism: 'PLAIN', Username: secrets.username, Password: secrets.password },
    },
    // eslint-disable-next-line camelcase
    sasl_ssl: {
        Type: 'sasl_ssl',
        Sasl: { Mechanism: 'SCRAM-SHA-512', Username: secrets.username, Password: secrets.password },
        Tls: { CaCertificate: secrets.caCertificate },
    },
    mtls: {
        Type: 'mtls',
        Tls: {
            CaCertificate: secrets.caCertificate,
            ClientCertificate: secrets.clientCertificate,
            ClientKey: secrets.clientKey,
            ClientKeyPassword: secrets.clientKeyPassword,
        },
    },
};

function makeRecord(overrides = {}) {
    return {
        modelVersion: 1,
        Name: 'orders',
        AccountId: accountId,
        Arn: `arn:scality:bucketnotif::${accountId}:orders`,
        Type: 'kafka',
        Connection: {
            Brokers: ['broker1.example.com:9093', '[fd00::1]:9092'],
            Topic: 'bucket-events',
            RequiredAcks: -1,
            CompressionType: 'zstd',
        },
        Auth: auths.sasl_ssl,
        CreatedAt: '2026-11-10T09:15:00Z',
        UpdatedAt: '2026-11-10T10:30:00Z',
        ...overrides,
    };
}

describe('destination XML', () => {
    it('should give every field of the record, in order, with no xmlns', () => {
        assert.strictEqual(
            destinationXml(makeRecord()),
            '<NotificationDestination>' +
                '<Name>orders</Name>' +
                `<Arn>arn:scality:bucketnotif::${accountId}:orders</Arn>` +
                `<AccountId>${accountId}</AccountId>` +
                '<Type>kafka</Type>' +
                '<Connection>' +
                '<Brokers><Broker>broker1.example.com:9093</Broker><Broker>[fd00::1]:9092</Broker></Brokers>' +
                '<Topic>bucket-events</Topic>' +
                '<RequiredAcks>-1</RequiredAcks>' +
                '<CompressionType>zstd</CompressionType>' +
                '</Connection>' +
                '<Auth><Type>sasl_ssl</Type></Auth>' +
                '<CreatedAt>2026-11-10T09:15:00Z</CreatedAt>' +
                '<UpdatedAt>2026-11-10T10:30:00Z</UpdatedAt>' +
                '</NotificationDestination>',
        );
    });

    it('should keep the brokers in their stored order', () => {
        const brokers = ['c.example.com:9092', 'a.example.com:9092', 'b.example.com:9092'];
        const xml = destinationXml(makeRecord({ Connection: { ...makeRecord().Connection, Brokers: brokers } }));
        assert.match(
            xml,
            new RegExp(`<Brokers>${brokers.map(broker => `<Broker>${broker}</Broker>`).join('')}</Brokers>`),
        );
    });

    it('should give RequiredAcks 0 and 1 as numbers', () => {
        [0, 1].forEach(requiredAcks => {
            const xml = destinationXml(
                makeRecord({ Connection: { ...makeRecord().Connection, RequiredAcks: requiredAcks } }),
            );
            assert.match(xml, new RegExp(`<RequiredAcks>${requiredAcks}</RequiredAcks>`));
        });
    });

    Object.keys(auths).forEach(authType => {
        it(`should reduce an ${authType} Auth to its Type and leak no secret`, () => {
            const xml = destinationXml(makeRecord({ Auth: auths[authType] }));
            assert.match(xml, new RegExp(`<Auth><Type>${authType}</Type></Auth>`));
            assert.doesNotMatch(xml, /<Sasl>|<Tls>|<Mechanism>|<Username>|<Password>/);
            Object.values(secrets).forEach(secret => assert.doesNotMatch(xml, new RegExp(secret)));
        });
    });

    it('should leave out a field that the record holds but the answer does not name', () => {
        const xml = destinationXml(makeRecord({ Extra: 'sentinel-extra-5c1e' }));
        assert.doesNotMatch(xml, /modelVersion|Extra|sentinel-extra-5c1e/);
    });

    ['Name', 'Arn', 'AccountId', 'Type', 'CreatedAt', 'UpdatedAt'].forEach(field => {
        it(`should throw on a record with no ${field}, rather than answer undefined`, () => {
            const record = makeRecord();
            delete record[field];
            assert.throws(() => destinationXml(record), new RegExp(field));
        });
    });

    [
        ['Connection', 'Topic'],
        ['Connection', 'RequiredAcks'],
        ['Connection', 'CompressionType'],
        ['Auth', 'Type'],
    ].forEach(([block, field]) => {
        it(`should throw on a record with no ${block}.${field}`, () => {
            const record = makeRecord();
            // a copy, so that the shared blocks of makeRecord stay whole
            record[block] = { ...record[block] };
            delete record[block][field];
            assert.throws(() => destinationXml(record), new RegExp(field));
        });
    });

    it('should escape every value', () => {
        const record = makeRecord({
            Name: 'a&b<c>d"e\'f',
            Arn: 'arn&<',
            AccountId: '<1>',
            Type: 'k&a',
            Connection: {
                Brokers: ['<b>:1', 'b&c:2'],
                Topic: 't"<',
                RequiredAcks: -1,
                CompressionType: "c'>",
            },
            Auth: { Type: 'n<o>' },
            CreatedAt: '<c>',
            UpdatedAt: '&u',
        });
        const xml = destinationXml(record);
        assert.strictEqual(
            xml,
            '<NotificationDestination>' +
                '<Name>a&amp;b&lt;c&gt;d&quot;e&apos;f</Name>' +
                '<Arn>arn&amp;&lt;</Arn>' +
                '<AccountId>&lt;1&gt;</AccountId>' +
                '<Type>k&amp;a</Type>' +
                '<Connection>' +
                '<Brokers><Broker>&lt;b&gt;:1</Broker><Broker>b&amp;c:2</Broker></Brokers>' +
                '<Topic>t&quot;&lt;</Topic>' +
                '<RequiredAcks>-1</RequiredAcks>' +
                '<CompressionType>c&apos;&gt;</CompressionType>' +
                '</Connection>' +
                '<Auth><Type>n&lt;o&gt;</Type></Auth>' +
                '<CreatedAt>&lt;c&gt;</CreatedAt>' +
                '<UpdatedAt>&amp;u</UpdatedAt>' +
                '</NotificationDestination>',
        );
    });
});
