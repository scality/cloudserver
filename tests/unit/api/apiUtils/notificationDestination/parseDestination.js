const assert = require('assert');
const xml2js = require('xml2js');

const parseDestination = require('../../../../../lib/api/apiUtils/notificationDestination/parseDestination');

const saslSslExample = {
    Type: 'kafka',
    Connection: {
        Brokers: { Broker: ['broker1.example.com:9093', 'broker2.example.com:9093'] },
        Topic: 'bucket-events',
        RequiredAcks: '-1',
        CompressionType: 'zstd',
    },
    Auth: {
        Type: 'sasl_ssl',
        Sasl: { Mechanism: 'SCRAM-SHA-512', Username: 'events', Password: 'secret' },
        Tls: { CaCertificate: '-----BEGIN CERTIFICATE-----...' },
    },
};

// Build the XML of a body from a plain object, in key order, so that each
// case reads as the body it sends. A value of undefined drops the element,
// and a list repeats it.
function toXml(name, value) {
    if (value === undefined) {
        return '';
    }
    if (Array.isArray(value)) {
        return value.map(item => toXml(name, item)).join('');
    }
    if (value !== null && typeof value === 'object') {
        const children = Object.keys(value)
            .map(key => toXml(key, value[key]))
            .join('');
        return `<${name}>${children}</${name}>`;
    }
    return `<${name}>${value}</${name}>`;
}

function parseBody(xml) {
    let parsed;
    xml2js.parseString(xml, (err, result) => {
        assert.ifError(err);
        parsed = result;
    });
    return parseDestination(parsed);
}

function check(body) {
    return parseBody(toXml('NotificationDestination', body));
}

// Return the body of the example with some fields replaced. A value of
// undefined drops that element.
function withFields(changes) {
    const body = JSON.parse(JSON.stringify(saslSslExample));
    Object.keys(changes).forEach(path => {
        const fields = path.split('.');
        const last = fields.pop();
        const parent = fields.reduce((node, field) => node[field], body);
        parent[last] = changes[path];
    });
    return body;
}

// The description starts with the full name of the field at fault, so that
// a case never passes on an error about another field.
function assertInvalid(result, field) {
    assert(result.error, `expected an error, got ${JSON.stringify(result)}`);
    assert.strictEqual(result.error.message, 'InvalidDestination');
    assert.strictEqual(result.error.code, 400);
    assert.match(result.error.description, new RegExp(`^${field.replace(/\./g, '\\.')} `));
}

describe('parseDestination', () => {
    describe('valid bodies', () => {
        it('should return the record fields of the sasl_ssl example, with Brokers as a list', () => {
            const result = check(saslSslExample);
            assert.strictEqual(result.error, undefined);
            assert.deepStrictEqual(result, {
                Type: 'kafka',
                Connection: {
                    Brokers: ['broker1.example.com:9093', 'broker2.example.com:9093'],
                    Topic: 'bucket-events',
                    RequiredAcks: -1,
                    CompressionType: 'zstd',
                },
                Auth: {
                    Type: 'sasl_ssl',
                    Sasl: { Mechanism: 'SCRAM-SHA-512', Username: 'events', Password: 'secret' },
                    Tls: { CaCertificate: '-----BEGIN CERTIFICATE-----...' },
                },
            });
        });

        it('should accept a root element with an xmlns', () => {
            const xml = toXml('NotificationDestination', saslSslExample).replace(
                '<NotificationDestination>',
                '<NotificationDestination xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
            );
            const result = parseBody(xml);
            assert.strictEqual(result.error, undefined);
            assert.strictEqual(result.Type, 'kafka');
        });

        it('should default RequiredAcks to -1 and CompressionType to none', () => {
            const result = check(
                withFields({ 'Connection.RequiredAcks': undefined, 'Connection.CompressionType': undefined }),
            );
            assert.strictEqual(result.error, undefined);
            assert.strictEqual(result.Connection.RequiredAcks, -1);
            assert.strictEqual(result.Connection.CompressionType, 'none');
        });

        ['-1', '0', '1'].forEach(acks => {
            it(`should accept RequiredAcks ${acks}`, () => {
                const result = check(withFields({ 'Connection.RequiredAcks': acks }));
                assert.strictEqual(result.error, undefined);
                assert.strictEqual(result.Connection.RequiredAcks, Number(acks));
            });
        });

        ['none', 'gzip', 'snappy', 'lz4', 'zstd'].forEach(compression => {
            it(`should accept CompressionType ${compression}`, () => {
                const result = check(withFields({ 'Connection.CompressionType': compression }));
                assert.strictEqual(result.error, undefined);
                assert.strictEqual(result.Connection.CompressionType, compression);
            });
        });

        [
            ['one broker', ['broker1.example.com:9093']],
            ['ten brokers', Array.from({ length: 10 }, (_, i) => `broker${i}.example.com:9093`)],
            ['a host name with no dot', ['kafka:9092']],
            ['an IPv4 address', ['10.0.0.1:9092']],
            ['an IPv6 address in brackets', ['[fd00::1]:9092']],
            ['ports 1 and 65535', ['broker1.example.com:1', 'broker2.example.com:65535']],
        ].forEach(([what, brokers]) => {
            it(`should accept ${what}`, () => {
                const result = check(withFields({ 'Connection.Brokers.Broker': brokers }));
                assert.strictEqual(result.error, undefined);
                assert.deepStrictEqual(result.Connection.Brokers, brokers);
            });
        });

        ['a', 'bucket-events', 'Orders.v2_EU', 'a'.repeat(249), '...'].forEach(topic => {
            it(`should accept the Topic '${topic.length > 20 ? `${topic.length} letters` : topic}'`, () => {
                const result = check(withFields({ 'Connection.Topic': topic }));
                assert.strictEqual(result.error, undefined);
                assert.strictEqual(result.Connection.Topic, topic);
            });
        });

        ['PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512'].forEach(mechanism => {
            it(`should accept the SASL mechanism ${mechanism}`, () => {
                const result = check(withFields({ 'Auth.Sasl.Mechanism': mechanism }));
                assert.strictEqual(result.error, undefined);
                assert.strictEqual(result.Auth.Sasl.Mechanism, mechanism);
            });
        });

        it('should accept Auth none, and leave Sasl and Tls absent', () => {
            const result = check(withFields({ Auth: { Type: 'none' } }));
            assert.strictEqual(result.error, undefined);
            assert.deepStrictEqual(result.Auth, { Type: 'none' });
        });

        it('should accept sasl_plaintext with a Sasl block', () => {
            const result = check(withFields({ 'Auth.Type': 'sasl_plaintext', 'Auth.Tls': undefined }));
            assert.strictEqual(result.error, undefined);
            assert.deepStrictEqual(result.Auth, {
                Type: 'sasl_plaintext',
                Sasl: { Mechanism: 'SCRAM-SHA-512', Username: 'events', Password: 'secret' },
            });
        });

        it('should accept sasl_ssl with no Tls block', () => {
            const result = check(withFields({ 'Auth.Tls': undefined }));
            assert.strictEqual(result.error, undefined);
            assert.strictEqual(result.Auth.Tls, undefined);
        });

        it('should accept mtls with a client certificate and key, and keep the optional fields', () => {
            const result = check(
                withFields({
                    Auth: {
                        Type: 'mtls',
                        Tls: {
                            CaCertificate: 'ca',
                            ClientCertificate: 'cert',
                            ClientKey: 'key',
                            ClientKeyPassword: 'pass',
                        },
                    },
                }),
            );
            assert.strictEqual(result.error, undefined);
            assert.deepStrictEqual(result.Auth, {
                Type: 'mtls',
                Tls: { CaCertificate: 'ca', ClientCertificate: 'cert', ClientKey: 'key', ClientKeyPassword: 'pass' },
            });
        });

        it('should accept mtls with no CA certificate and no key password', () => {
            const result = check(
                withFields({ Auth: { Type: 'mtls', Tls: { ClientCertificate: 'cert', ClientKey: 'key' } } }),
            );
            assert.strictEqual(result.error, undefined);
            assert.deepStrictEqual(result.Auth, { Type: 'mtls', Tls: { ClientCertificate: 'cert', ClientKey: 'key' } });
        });
    });

    describe('root', () => {
        it('should refuse another root element', () => {
            assertInvalid(parseBody(toXml('Destination', saslSslExample)), 'The root element');
        });

        it('should refuse an unknown element', () => {
            assertInvalid(check(withFields({ Extra: 'x' })), 'NotificationDestination.Extra');
        });

        it('should refuse a repeated element', () => {
            const xml = toXml('NotificationDestination', saslSslExample).replace(
                '<Type>kafka</Type>',
                '<Type>kafka</Type><Type>kafka</Type>',
            );
            assertInvalid(parseBody(xml), 'NotificationDestination.Type');
        });
    });

    describe('Type', () => {
        it('should refuse a Type other than kafka', () => {
            assertInvalid(check(withFields({ Type: 'rabbitmq' })), 'NotificationDestination.Type');
        });

        it('should refuse a missing Type', () => {
            assertInvalid(check(withFields({ Type: undefined })), 'NotificationDestination.Type');
        });
    });

    describe('Connection', () => {
        it('should refuse a missing Connection', () => {
            assertInvalid(check(withFields({ Connection: undefined })), 'Connection');
        });

        ['Brokers', 'Topic'].forEach(field => {
            it(`should refuse a missing ${field}`, () => {
                assertInvalid(check(withFields({ [`Connection.${field}`]: undefined })), `Connection.${field}`);
            });

            it(`should refuse an empty ${field}`, () => {
                assertInvalid(check(withFields({ [`Connection.${field}`]: '' })), `Connection.${field}`);
            });
        });

        it('should refuse Host and Port, which Brokers replaces', () => {
            const body = withFields({ 'Connection.Brokers': undefined });
            body.Connection = { Host: 'broker1.example.com', Port: '9093', ...body.Connection };
            assertInvalid(check(body), 'Connection.Host');
        });

        it('should refuse 11 brokers', () => {
            const brokers = Array.from({ length: 11 }, (_, i) => `broker${i}.example.com:9093`);
            assertInvalid(check(withFields({ 'Connection.Brokers.Broker': brokers })), 'Connection.Brokers');
        });

        it('should refuse an element other than Broker in Brokers', () => {
            assertInvalid(
                check(withFields({ 'Connection.Brokers': { Host: 'broker1.example.com:9093' } })),
                'Connection.Brokers.Host',
            );
        });

        [
            ['a broker with no port', 'broker1.example.com'],
            ['a broker with port 0', 'broker1.example.com:0'],
            ['a broker with port 65536', 'broker1.example.com:65536'],
            ['a broker with a port that is not a number', 'broker1.example.com:abc'],
            ['an IPv6 address with no brackets', 'fd00::1:9092'],
            ['an IPv4 address in brackets', '[10.0.0.1]:9092'],
            ['an IPv4 address out of range', '999.0.0.1:9092'],
            ['a short IPv4 address', '10.1:9092'],
            ['two brokers in one entry', 'broker1.example.com:9093,broker2.example.com:9093'],
            ['a host with a space', 'bad host:9092'],
            ['a broker with spaces around it', ' broker1.example.com:9093 '],
            ['a host with a slash', 'a/b:9092'],
            ['a host label that starts with a hyphen', '-broker.example.com:9092'],
            ['an empty Broker', ''],
        ].forEach(([what, broker]) => {
            it(`should refuse ${what}`, () => {
                assertInvalid(
                    check(withFields({ 'Connection.Brokers.Broker': ['broker1.example.com:9093', broker] })),
                    'Connection.Brokers.Broker',
                );
            });
        });

        ['127.0.0.1:9092', '[::1]:9092', '169.254.169.254:80', '0.0.0.0:9092', '[fe80::1]:9092', '[::]:9092'].forEach(
            broker => {
                it(`should refuse the local address ${broker}`, () => {
                    const result = check(withFields({ 'Connection.Brokers.Broker': [broker] }));
                    assertInvalid(result, 'Connection.Brokers.Broker');
                    assert.match(result.error.description, /loopback, link-local or unspecified/);
                });
            },
        );

        ['localhost:9092', 'kafka.localhost:9092', 'KAFKA.LOCALHOST:9092'].forEach(broker => {
            it(`should refuse the localhost name ${broker}`, () => {
                const result = check(withFields({ 'Connection.Brokers.Broker': ['broker1.example.com:9093', broker] }));
                assertInvalid(result, 'Connection.Brokers.Broker');
                assert.match(result.error.description, /localhost/);
            });
        });

        [
            '10.1.2.3:9092',
            '172.16.0.1:9092',
            '192.168.1.1:9092',
            '[fc00::1]:9092',
            'kafka.example.com:9092',
            'localhost.example.com:9092',
        ].forEach(broker => {
            it(`should accept the broker ${broker}`, () => {
                const result = check(withFields({ 'Connection.Brokers.Broker': [broker] }));
                assert.strictEqual(result.error, undefined);
            });
        });

        [
            ['a space', 'bucket events'],
            ['spaces around it', ' bucket-events '],
            ['250 letters', 'a'.repeat(250)],
            ['the name .', '.'],
            ['the name ..', '..'],
            ['a slash', 'bucket/events'],
        ].forEach(([what, topic]) => {
            it(`should refuse a Topic with ${what}`, () => {
                assertInvalid(check(withFields({ 'Connection.Topic': topic })), 'Connection.Topic');
            });
        });

        ['2', '-2', 'all', '1.0', ''].forEach(acks => {
            it(`should refuse RequiredAcks '${acks}'`, () => {
                assertInvalid(check(withFields({ 'Connection.RequiredAcks': acks })), 'Connection.RequiredAcks');
            });
        });

        ['brotli', 'ZSTD', ''].forEach(compression => {
            it(`should refuse CompressionType '${compression}'`, () => {
                assertInvalid(
                    check(withFields({ 'Connection.CompressionType': compression })),
                    'Connection.CompressionType',
                );
            });
        });

        it('should refuse an unknown element in Connection', () => {
            assertInvalid(check(withFields({ 'Connection.Partition': '3' })), 'Connection.Partition');
        });
    });

    describe('Auth', () => {
        it('should refuse a missing Auth', () => {
            assertInvalid(check(withFields({ Auth: undefined })), 'Auth');
        });

        it('should refuse a missing Auth.Type', () => {
            assertInvalid(check(withFields({ 'Auth.Type': undefined })), 'Auth.Type');
        });

        it('should refuse an unknown Auth.Type', () => {
            assertInvalid(check(withFields({ 'Auth.Type': 'kerberos' })), 'Auth.Type');
        });

        it('should refuse an unknown element in Auth', () => {
            assertInvalid(check(withFields({ 'Auth.Token': 'x' })), 'Auth.Token');
        });

        ['sasl_plaintext', 'sasl_ssl'].forEach(type => {
            it(`should refuse ${type} with no Sasl block`, () => {
                assertInvalid(
                    check(withFields({ 'Auth.Type': type, 'Auth.Sasl': undefined, 'Auth.Tls': undefined })),
                    'Auth.Sasl',
                );
            });
        });

        ['Mechanism', 'Username', 'Password'].forEach(field => {
            it(`should refuse a Sasl block with no ${field}`, () => {
                assertInvalid(check(withFields({ [`Auth.Sasl.${field}`]: undefined })), `Auth.Sasl.${field}`);
            });
        });

        it('should refuse the GSSAPI mechanism, naming Sasl.Mechanism', () => {
            assertInvalid(check(withFields({ 'Auth.Sasl.Mechanism': 'GSSAPI' })), 'Auth.Sasl.Mechanism');
        });

        it('should refuse an unknown SASL mechanism', () => {
            assertInvalid(check(withFields({ 'Auth.Sasl.Mechanism': 'OAUTHBEARER' })), 'Auth.Sasl.Mechanism');
        });

        it('should refuse an unknown element in Sasl', () => {
            assertInvalid(check(withFields({ 'Auth.Sasl.Realm': 'x' })), 'Auth.Sasl.Realm');
        });

        ['none', 'mtls'].forEach(type => {
            it(`should refuse a Sasl block with ${type}`, () => {
                assertInvalid(
                    check(
                        withFields({
                            'Auth.Type': type,
                            'Auth.Tls': type === 'mtls' ? { ClientCertificate: 'cert', ClientKey: 'key' } : undefined,
                        }),
                    ),
                    'Auth.Sasl',
                );
            });
        });

        ['none', 'sasl_plaintext'].forEach(type => {
            it(`should refuse a Tls block with ${type}`, () => {
                assertInvalid(
                    check(
                        withFields({
                            'Auth.Type': type,
                            'Auth.Sasl': type === 'none' ? undefined : saslSslExample.Auth.Sasl,
                        }),
                    ),
                    'Auth.Tls',
                );
            });
        });

        it('should refuse an empty Tls block with sasl_ssl', () => {
            assertInvalid(check(withFields({ 'Auth.Tls': {} })), 'Auth.Tls.CaCertificate');
        });

        ['ClientCertificate', 'ClientKey', 'ClientKeyPassword'].forEach(field => {
            it(`should refuse a Tls ${field} with sasl_ssl`, () => {
                assertInvalid(check(withFields({ [`Auth.Tls.${field}`]: 'x' })), `Auth.Tls.${field}`);
            });
        });

        it('should refuse mtls with no Tls block', () => {
            assertInvalid(check(withFields({ Auth: { Type: 'mtls' } })), 'Auth.Tls');
        });

        it('should refuse mtls with no client key', () => {
            assertInvalid(
                check(withFields({ Auth: { Type: 'mtls', Tls: { CaCertificate: 'ca', ClientCertificate: 'cert' } } })),
                'Auth.Tls.ClientKey',
            );
        });

        it('should refuse mtls with no client certificate', () => {
            assertInvalid(
                check(withFields({ Auth: { Type: 'mtls', Tls: { CaCertificate: 'ca', ClientKey: 'key' } } })),
                'Auth.Tls.ClientCertificate',
            );
        });

        it('should refuse an unknown element in Tls', () => {
            assertInvalid(check(withFields({ 'Auth.Tls.Verify': 'false' })), 'Auth.Tls.Verify');
        });
    });

    it('should never put a secret in an error description', () => {
        const result = check(withFields({ 'Auth.Type': 'none', 'Auth.Tls': undefined }));
        assertInvalid(result, 'Auth.Sasl');
        assert.doesNotMatch(result.error.description, /secret/);
    });
});
