const assert = require('assert');

const { parseRateLimitConfig } = require('../../../../../lib/api/apiUtils/rateLimit/config');

describe('test parseRateLimitConfig', () => {
    const testCases = [
        {
            desc: 'should return an empty config if given one',
            input: {},
            expected: {},
        },
        {
            desc: '[rps] should calculate the request interval',
            input: {
                requestsPerSecond: {
                    limit: 500
                },
            },
            expected: {
                requestsPerSecond: {
                    interval: 2,
                    bucketSize: 1000,
                }
            },
        },
        {
            desc: '[rps] should calculate bucket size',
            input: {
                requestsPerSecond: {
                    limit: 500,
                    burstCapacity: 5,
                },
            },
            expected: {
                requestsPerSecond: {
                    interval: 2,
                    bucketSize: 5000,
                }
            },
        },
        {
            desc: '[rps] should throw an error if requestsPerSecond isn\'t an object',
            input: {
                requestsPerSecond: 'foo'
            },
            throws: true,
        },
        {
            desc: '[rps] should throw an error if requestsPerSecond.limit isn\'t a number',
            input: {
                requestsPerSecond: {
                    limit: 'foo',
                },
            },
            throws: true,
        },
        {
            desc: '[rps] should throw an error if requestsPerSecond.burstCapacity isn\'t a number',
            input: {
                requestsPerSecond: {
                    limit: 500,
                    burstCapacity: '5',
                },
            },
            throws: true,
        },
    ];

    testCases.forEach(testCase => {
        it(testCase.desc, () => {
            if (testCase.throws) {
                let err;
                try {
                    parseRateLimitConfig(testCase.input);
                } catch (e) {
                    err = e;
                }
                assert(err instanceof Error);
            } else {
                assert.deepStrictEqual(parseRateLimitConfig(testCase.input), testCase.expected);
            }
        });
    });
});
