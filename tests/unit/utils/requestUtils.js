const assert = require('assert');
const DummyRequest = require('../DummyRequest');
const requestUtils = require('../../../lib/utilities/requestUtils');

describe('requestUtils.getClientIp', () => {
    const testClientIpFromProxy = '123.123.123.123';
    const testClientIp = '10.10.10.10';

    it('should return client Ip address from x-forwarded-for header ' +
        'if the request comes via proxies', () => {
        const request = new DummyRequest({
            headers: {
                'x-forwarded-for': `${testClientIpFromProxy}, ${testClientIp}`,
            },
            url: '/',
            parsedHost: 'localhost',
            socket: {
                remoteAddress: testClientIp,
            },
        });
        const result = requestUtils.getClientIp(request);
        assert.strictEqual(result, testClientIpFromProxy);
    });

    it('should return client Ip address from socket ' +
        'if the request is not forwarded from proxies', () => {
        const request = new DummyRequest({
            url: '/',
            parsedHost: 'localhost',
            socket: {
                remoteAddress: testClientIp,
            },
        });
        const result = requestUtils.getClientIp(request);
        assert.strictEqual(result, testClientIp);
    });
});
