const assert = require('assert');
const { parseRedisConfig } = require('../../../lib/Config');

describe('parseRedisConfig', () => {
    [
        {
            desc: 'with host and port',
            input: {
                host: 'localhost',
                port: 6479,
            },
        },
        {
            desc: 'with host, port and password',
            input: {
                host: 'localhost',
                port: 6479,
                password: 'mypass',
            },
        },
        {
            desc: 'with host, port and an empty password',
            input: {
                host: 'localhost',
                port: 6479,
                password: '',
            },
        },
        {
            desc: 'with host, port and an empty retry config',
            input: {
                host: 'localhost',
                port: 6479,
                retry: {
                },
            },
        },
        {
            desc: 'with host, port and a custom retry config',
            input: {
                host: 'localhost',
                port: 6479,
                retry: {
                    connectBackoff: {
                        min: 10,
                        max: 1000,
                        jitter: 0.1,
                        factor: 1.5,
                        deadline: 10000,
                    },
                },
            },
        },
        {
            desc: 'with a single sentinel and no sentinel password',
            input: {
                name: 'myname',
                sentinels: [
                    {
                        host: 'localhost',
                        port: 16479,
                    },
                ],
            },
        },
        {
            desc: 'with two sentinels and a sentinel password',
            input: {
                name: 'myname',
                sentinels: [
                    {
                        host: '10.20.30.40',
                        port: 16479,
                    },
                    {
                        host: '10.20.30.41',
                        port: 16479,
                    },
                ],
                sentinelPassword: 'mypass',
            },
        },
        {
            desc: 'with a sentinel and an empty sentinel password',
            input: {
                name: 'myname',
                sentinels: [
                    {
                        host: '10.20.30.40',
                        port: 16479,
                    },
                ],
                sentinelPassword: '',
            },
        },
        {
            desc: 'with a basic production-like config with sentinels',
            input: {
                name: 'scality-s3',
                password: '',
                sentinelPassword: '',
                sentinels: [
                    {
                        host: 'storage-1',
                        port: 16379,
                    },
                    {
                        host: 'storage-2',
                        port: 16379,
                    },
                    {
                        host: 'storage-3',
                        port: 16379,
                    },
                    {
                        host: 'storage-4',
                        port: 16379,
                    },
                    {
                        host: 'storage-5',
                        port: 16379,
                    },
                ],
            },
        },
        {
            desc: 'with a single sentinel passed as a string',
            input: {
                name: 'myname',
                sentinels: '10.20.30.40:16479',
            },
            output: {
                name: 'myname',
                sentinels: [
                    {
                        host: '10.20.30.40',
                        port: 16479,
                    },
                ],
            },
        },
        {
            desc: 'with a list of sentinels passed as a string',
            input: {
                name: 'myname',
                sentinels: '10.20.30.40:16479,another-host:16480,10.20.30.42:16481',
                sentinelPassword: 'mypass',
            },
            output: {
                name: 'myname',
                sentinels: [
                    {
                        host: '10.20.30.40',
                        port: 16479,
                    },
                    {
                        host: 'another-host',
                        port: 16480,
                    },
                    {
                        host: '10.20.30.42',
                        port: 16481,
                    },
                ],
                sentinelPassword: 'mypass',
            },
        },
    ].forEach(testCase => {
        it(`should parse a valid config ${testCase.desc}`, () => {
            const redisConfig = parseRedisConfig(testCase.input);
            assert.deepStrictEqual(redisConfig, testCase.output || testCase.input);
        });
    });

    [
        {
            desc: 'that is empty',
            input: {},
        },
        {
            desc: 'with only a host',
            input: {
                host: 'localhost',
            },
        },
        {
            desc: 'with only a port',
            input: {
                port: 6479,
            },
        },
        {
            desc: 'with a custom retry config with missing values',
            input: {
                host: 'localhost',
                port: 6479,
                retry: {
                    connectBackoff: {
                    },
                },
            },
        },
        {
            desc: 'with a sentinel but no name',
            input: {
                sentinels: [
                    {
                        host: 'localhost',
                        port: 16479,
                    },
                ],
            },
        },
        {
            desc: 'with a sentinel but an empty name',
            input: {
                name: '',
                sentinels: [
                    {
                        host: 'localhost',
                        port: 16479,
                    },
                ],
            },
        },
        {
            desc: 'with an empty list of sentinels',
            input: {
                name: 'myname',
                sentinels: [],
            },
        },
        {
            desc: 'with an empty list of sentinels passed as a string',
            input: {
                name: 'myname',
                sentinels: '',
            },
        },
        {
            desc: 'with an invalid list of sentinels passed as a string (missing port)',
            input: {
                name: 'myname',
                sentinels: '10.20.30.40:16479,10.20.30.50',
            },
        },
    ].forEach(testCase => {
        it(`should fail to parse an invalid config ${testCase.desc}`, () => {
            assert.throws(() => {
                parseRedisConfig(testCase.input);
            });
        });
    });
});
