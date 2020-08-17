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
    it('should throw an error if auth is not an object', () => {
        assert.throws(() => {
            bucketNotifAssert([{
                resource: 'target1',
                type: 'kafka',
                host: 'localhost',
                port: 8000,
                auth: 'yes',
            }]);
        }, '/bad config: bucket notification auth must be an object/');
    });
    it('should throw an error if auth is an empty object', () => {
        assert.throws(() => {
            bucketNotifAssert([{
                resource: 'target1',
                type: 'kafka',
                host: 'localhost',
                port: 8000,
                auth: {},
            }]);
        }, '/bad config: bucket notification configuration ' +
        'auth should contain either cert or user and password/');
    });
    it('should throw an error if auth includes cert, user, and password', () => {
        assert.throws(() => {
            bucketNotifAssert([{
                resource: 'target1',
                type: 'kafka',
                host: 'localhost',
                port: 8000,
                auth: { user: 'user', password: 'password', cert: 'cert/path' },
            }]);
        }, '/bad config: bucket notification configuration ' +
        'auth should contain either cert or user and password/');
    });
    it('should throw an error if auth includes user but no password', () => {
        assert.throws(() => {
            bucketNotifAssert([{
                resource: 'target1',
                type: 'kafka',
                host: 'localhost',
                port: 8000,
                auth: { user: 'user' },
            }]);
        }, '/bad config: bucket notification configuration ' +
        'auth should contain both user and password if not using cert/');
    });
    it('should throw an error if auth user is not a string', () => {
        assert.throws(() => {
            bucketNotifAssert([{
                resource: 'target1',
                type: 'kafka',
                host: 'localhost',
                port: 8000,
                auth: { user: 1, password: 'password' },
            }]);
        }, '/bad config: bucket notification configuration auth user should be a string/');
    });
    it('should throw an error if auth password is not a string', () => {
        assert.throws(() => {
            bucketNotifAssert([{
                resource: 'target1',
                type: 'kafka',
                host: 'localhost',
                port: 8000,
                auth: { user: 'user', password: 12345 },
            }]);
        }, '/bad config: bucket notification configuration auth password should be a string/');
    });
});
