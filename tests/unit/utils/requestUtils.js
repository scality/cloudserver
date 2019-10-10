const assert = require('assert');
const path = require('path');
const DummyRequest = require('../DummyRequest');

describe('requestUtils.getClientIp', () => {
    const testConfigFilePath = '../../unit/utils/requests-test-proxy.json';
    const config = require(testConfigFilePath);
    const testClientIp1 = '10.10.10.10';
    const testClientIp2 = '10.10.10.11';
    let testProxyIp;
    let configFilePath;
    let requestUtils;

    beforeEach(() => {
        testProxyIp = config.requests.trustedProxyIPs;
        configFilePath = process.env.S3_CONFIG_FILE;
        process.env.S3_CONFIG_FILE = path.join(__dirname, testConfigFilePath);
        requestUtils = require('../../../lib/utilities/requestUtils');
    });

    afterEach(() => {
        if (configFilePath) {
            process.env.S3_CONFIG_FILE = configFilePath;
        }
    });

    it('should return client Ip address from header ' +
        'if the request comes via proxies', () => {
        const request = new DummyRequest({
            headers: {
                'x-forwarded-for': [testClientIp1, ...testProxyIp].join(','),
            },
            url: '/',
            parsedHost: 'localhost',
            socket: {
                remoteAddress: testClientIp1,
            },
        });
        const result = requestUtils.getClientIp(request);
        assert.strictEqual(result, testClientIp1);
    });

    it('should return client Ip address from socket ' +
        'if the request is not forwarded from proxies', () => {
        const request = new DummyRequest({
            headers: {
                'x-forwarded-for': [testClientIp1, ...testProxyIp].join(','),
            },
            url: '/',
            parsedHost: 'localhost',
            socket: {
                remoteAddress: testClientIp2,
            },
        });
        const result = requestUtils.getClientIp(request);
        assert.strictEqual(result, testClientIp2);
    });
});
