const assert = require('assert');
const { bucketNotifAssert } = require('../../../lib/Config');

describe('bucketNotifAssert', () => {
    it('should not throw an error if bucket notification config is valid', () => {
        bucketNotifAssert([{
            resource: 'target1',
            type: 'kafka',
            host: 'localhost',
            port: 8000,
            auth: { user: 'user', password: 'password' },
        }]);
    });
    it('should throw an error if bucket notification config is not an array', () => {
        assert.throws(() => {
            bucketNotifAssert({
                resource: 'target1',
                type: 'kafka',
                host: 'localhost',
                port: 8000,
                auth: { user: 'user', password: 'password' },
            });
        },
            '/bad config: bucket notification configuration must be an array/');
    });
    it('should throw an error if resource is not a string', () => {
        assert.throws(() => {
            bucketNotifAssert([{
                resource: 12345,
                type: 'kafka',
                host: 'localhost',
                port: 8000,
                auth: { user: 'user', password: 'password' },
            }]);
        }, '/bad config: bucket notification configuration resource must be a string/');
    });
    it('should throw an error if type is not a string', () => {
        assert.throws(() => {
            bucketNotifAssert([{
                resource: 'target1',
                type: 12345,
                host: 'localhost',
                port: 8000,
                auth: { user: 'user', password: 'password' },
            }]);
        }, '/bad config: bucket notification configuration type must be a string/');
    });
    it('should throw an error if host is not a string', () => {
        assert.throws(() => {
            bucketNotifAssert([{
                resource: 'target1',
                type: 'kafka',
                host: 127.0,
                port: 8000,
                auth: { user: 'user', password: 'password' },
            }]);
        }, '/bad config: bucket notification configuration type must be a string/');
    });
    it('should throw an error if port is not an integer', () => {
        assert.throws(() => {
            bucketNotifAssert([{
                resource: 'target1',
                type: 'kafka',
                host: 'localhost',
                port: '8000',
                auth: { user: 'user', password: 'password' },
            }]);
        }, '/bad config: port must be a positive integer/');
    });
    // TODO: currently auth is fluid and once a concrete structure is
    // established, add tests to auth part of the config
});
