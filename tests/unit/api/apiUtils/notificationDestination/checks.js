const assert = require('assert');

const {
    isLocalAddress,
    isValidHostPort,
    isValidKafkaTopic,
} = require('../../../../../lib/api/apiUtils/notificationDestination/checks');

describe('notification destination checks', () => {
    describe('isValidHostPort', () => {
        [
            'broker1.example.com:9093',
            'kafka:9092',
            'localhost:1',
            'localhost:65535',
            '10.0.0.1:9092',
            '[::1]:9000',
            '[fd00::1]:65535',
            '[::ffff:10.0.0.1]:9092',
            '0.0.0.0:9092',
            'broker-1.kafka2:9092',
            `${'a'.repeat(63)}.example.com:9092`,
        ].forEach(entry => {
            it(`should accept '${entry.length > 40 ? `${entry.slice(0, 20)}...` : entry}'`, () => {
                assert.strictEqual(isValidHostPort(entry), true);
            });
        });

        [
            ['a host with no port', 'localhost'],
            ['an empty string', ''],
            ['a port and no host', ':9092'],
            ['port 0', 'localhost:0'],
            ['port 65536', 'localhost:65536'],
            ['a port with six digits', 'localhost:009092'],
            ['a port that is not a number', 'localhost:abc'],
            ['a negative port', 'localhost:-1'],
            ['an IPv6 address with no brackets', '::1:9000'],
            ['an IPv4 address in brackets', '[10.0.0.1]:9092'],
            ['an IPv6 address that does not parse', '[fd00::zz]:9092'],
            ['an IPv4 address out of range', '256.0.0.1:9092'],
            ['an IPv4 address with a leading zero', '010.0.0.1:9092'],
            ['an IPv4 address with a leading zero last', '1.2.3.04:9092'],
            ['an IPv4 address in hex', '0x7f.0.0.1:9092'],
            ['a host name whose last label is a number', 'a.1:9092'],
            ['a host name whose last label is hex', 'a.0x1:9092'],
            ['an IPv6 address with a zone ID', '[fe80::1%eth0]:9092'],
            ['an embedded IPv4 address with a leading zero', '[::ffff:010.0.0.1]:9092'],
            ['a short IPv4 address', '1.2.3:9092'],
            ['two entries with a comma', 'a:1,b:2'],
            ['a space', 'bad host:9092'],
            ['spaces around it', ' localhost:9092 '],
            ['a slash', 'a/b:9092'],
            ['a label that starts with a hyphen', '-a.example.com:9092'],
            ['a label of 64 letters', `${'a'.repeat(64)}.example.com:9092`],
            ['a trailing dot', 'example.com.:9092'],
            ['two ports', 'localhost:9092:9093'],
            ['an underscore', 'bucketd_1:9092'],
        ].forEach(([what, entry]) => {
            it(`should refuse ${what}`, () => {
                assert.strictEqual(isValidHostPort(entry), false);
            });
        });

        [undefined, null, 9092, ['localhost:9092'], { host: 'localhost' }].forEach(value => {
            it(`should refuse the non-string ${JSON.stringify(value)}`, () => {
                assert.strictEqual(isValidHostPort(value), false);
            });
        });
    });

    describe('isLocalAddress', () => {
        [
            '127.0.0.1:9092',
            '127.5.0.1:9092',
            '[::1]:9092',
            '169.254.169.254:80',
            '[fe80::1]:9092',
            '0.0.0.0:9092',
            '[::]:9092',
            '[::ffff:127.0.0.1]:9092',
            // IPv4-mapped IPv6 addresses in hex form: 127.0.0.1,
            // 169.254.169.254 and 0.0.0.0
            '[::ffff:7f00:1]:9092',
            '[::ffff:a9fe:a9fe]:80',
            '[::ffff:0:0]:9092',
            'localhost:9092',
            'LocalHost:9092',
            'kafka.localhost:9092',
            'LOCALHOST.:9092',
            'kafka.localhost.:9092',
        ].forEach(entry => {
            it(`should find '${entry}' local`, () => {
                assert.strictEqual(isLocalAddress(entry), true);
            });
        });

        [
            '10.1.2.3:9092',
            '172.16.0.1:9092',
            '192.168.1.1:9092',
            '[fc00::1]:9092',
            // IPv4-mapped IPv6 addresses of private ranges: 10.1.2.3 and
            // 192.168.1.1
            '[::ffff:a01:203]:9092',
            '[::ffff:192.168.1.1]:9092',
            '8.8.8.8:9092',
            'broker1.example.com:9092',
            'kafka.example.com:9092',
            'localhost.example.com:9092',
            'mylocalhost:9092',
        ].forEach(entry => {
            it(`should not find '${entry}' local`, () => {
                assert.strictEqual(isLocalAddress(entry), false);
            });
        });
    });

    describe('isValidKafkaTopic', () => {
        ['a', 'bucket-events', 'Orders.v2_EU', '...', 'a'.repeat(249)].forEach(topic => {
            it(`should accept '${topic.length > 20 ? `${topic.length} letters` : topic}'`, () => {
                assert.strictEqual(isValidKafkaTopic(topic), true);
            });
        });

        [
            ['an empty name', ''],
            ['the name .', '.'],
            ['the name ..', '..'],
            ['250 letters', 'a'.repeat(250)],
            ['a space', 'bucket events'],
            ['a slash', 'bucket/events'],
            ['a colon', 'bucket:events'],
            ['a non-string', 42],
        ].forEach(([what, topic]) => {
            it(`should refuse ${what}`, () => {
                assert.strictEqual(isValidKafkaTopic(topic), false);
            });
        });
    });
});
