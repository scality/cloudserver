const assert = require('assert');

describe('Config', () => {
    it('should load default config.json without errors', done => {
        require('../../lib/Config');
        done();
    });

    it('should emit an event when auth data is updated and append new ' +
    'accounts to existing accounts', done => {
        const { ConfigObject } = require('../../lib/Config');
        const config = new ConfigObject();

        const existingDataCount = config.authData.accounts.length;
        let emitted = false;

        config.on('authdata-update', () => {
            const newCount = config.authData.accounts.length;
            assert.strictEqual(existingDataCount + 1, newCount);

            emitted = true;
        });

        const newAcct = {
            name: 'Bart2',
            email: 'sampleaccount1@sampling.com',
            arn: 'arn:aws:iam::123456789016:root',
            canonicalID:
            '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2bf',
            shortid: '123456789016',
            keys: [{ access: 'accessKey1', secret: 'verySecretKey1' }],
        };
        config.setAuthDataAccounts([newAcct]);

        if (emitted) {
            return done();
        }
        return done(new Error('authdata-update event was not emitted'));
    });
});

describe('Config::_normalizeRestEndpoints', () => {
    const tests = [
        {
            msg: 'should return an object with read/write locations if given ' +
            'a string preferred location',
            input: { stringendpoint: 'us-east-1' },
            output: {
                stringendpoint: {
                    read: 'us-east-1',
                    write: 'us-east-1',
                },
            },
        },
        {
            msg: 'should return an object with read/write locations if given ' +
            'an object with read/write preference',
            input: {
                objectendpoint: {
                    read: 'us-east-1',
                    write: 'us-east-1',
                },
            },
            output: {
                objectendpoint: {
                    read: 'us-east-1',
                    write: 'us-east-1',
                },
            },
        },
        {
            msg: 'should return an object with read/write locations if given ' +
            'an object with different read/write preferences',
            input: {
                objectendpoint: {
                    read: 'us-east-1',
                    write: 'us-east-2',
                },
            },
            output: {
                objectendpoint: {
                    read: 'us-east-1',
                    write: 'us-east-2',
                },
            },
        },
    ];

    let config;
    before(() => {
        const { ConfigObject } = require('../../lib/Config');
        config = new ConfigObject();
    });

    tests.forEach(test => it(test.msg, () => {
        const restEndpoints = config._normalizeRestEndpoints(
            test.input, config.locationConstraints);
        assert.deepStrictEqual(restEndpoints, test.output);
    }));
});
