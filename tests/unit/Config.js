const assert = require('assert');

describe('Config', () => {
    it('should load default config.json without errors', done => {
        require('../../lib/Config');
        done();
    });

    it('should emit an event when auth data is updated', done => {
        const { ConfigObject } = require('../../lib/Config');
        const config = new ConfigObject();
        let emitted = false;
        config.on('authdata-update', () => {
            emitted = true;
        });
        config.setAuthDataAccounts([]);
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
