const assert = require('assert');
const DummyRequest = require('../DummyRequest');
const requestUtils = require('../../../lib/utilities/requestUtils');

describe('requestUtils.getClientIp', () => {
    const testConfig1 = require('../../unit/utils/requests-test-proxy.json');
    const testConfig2 = require('../../../config.json');
    const testClientIp1 = '192.168.100.1';
    const testClientIp2 = '192.168.104.0';
    const testProxyIp = '192.168.100.2';

    it('should return client Ip address from header ' +
        'if the request comes via proxies', () => {
        const request = new DummyRequest({
            headers: {
                'x-forwarded-for': [testClientIp1, testProxyIp].join(','),
            },
            url: '/',
            parsedHost: 'localhost',
            socket: {
                remoteAddress: testProxyIp,
            },
        });
        const result = requestUtils.getClientIp(request, testConfig1);
        assert.strictEqual(result, testClientIp1);
    });

    it('should not return client Ip address from header ' +
        'if the request is not forwarded from proxies or ' +
        'fails ip check', () => {
        const request = new DummyRequest({
            headers: {
                'x-forwarded-for': [testClientIp1, testProxyIp].join(','),
            },
            url: '/',
            parsedHost: 'localhost',
            socket: {
                remoteAddress: testClientIp2,
            },
        });
        const result = requestUtils.getClientIp(request, testConfig2);
        assert.strictEqual(result, testClientIp2);
    });
});
