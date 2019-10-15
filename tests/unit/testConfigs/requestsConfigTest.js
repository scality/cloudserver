const assert = require('assert');
const { requestsConfigAssert } = require('../../../lib/Config');

describe('requestsConfigAssert', () => {
    it('should not throw an error if there is no requests config', () => {
        assert.doesNotThrow(() => {
            requestsConfigAssert({});
        },
        'should not throw an error if there is no requests config');
    });
    it('should not throw an error if requests config via proxy is set to false',
    () => {
        assert.doesNotThrow(() => {
            requestsConfigAssert({
                viaProxy: false,
                trustedProxyCIDRs: [],
                extractClientIPFromHeader: '',
            });
        },
        'shouldnt throw an error if requests config via proxy is set to false');
    });
    it('should not throw an error if requests config via proxy is true, ' +
        'trustedProxyCIDRs & extractClientIPFromHeader are set', () => {
        assert.doesNotThrow(() => {
            requestsConfigAssert({
                viaProxy: true,
                trustedProxyCIDRs: ['123.123.123.123'],
                extractClientIPFromHeader: 'x-forwarded-for',
            });
        },
        'should not throw an error if requests config ' +
        'via proxy is set correctly');
    });
    it('should throw an error if requests.viaProxy is not a boolean',
    () => {
        assert.throws(() => {
            requestsConfigAssert({
                viaProxy: 1,
                trustedProxyCIDRs: ['123.123.123.123'],
                extractClientIPFromHeader: 'x-forwarded-for',
            });
        },
        '/config: invalid requests configuration. viaProxy must be a ' +
        'boolean/');
    });
    it('should throw an error if requests.trustedProxyCIDRs is not an array',
    () => {
        assert.throws(() => {
            requestsConfigAssert({
                viaProxy: true,
                trustedProxyCIDRs: 1,
                extractClientIPFromHeader: 'x-forwarded-for',
            });
        },
        '/config: invalid requests configuration. ' +
        'trustedProxyCIDRs must be set if viaProxy is set to true ' +
        'and must be an array/');
    });
    it('should throw an error if requests.trustedProxyCIDRs array is empty',
    () => {
        assert.throws(() => {
            requestsConfigAssert({
                viaProxy: true,
                trustedProxyCIDRs: [],
                extractClientIPFromHeader: 'x-forwarded-for',
            });
        },
        '/config: invalid requests configuration. ' +
        'trustedProxyCIDRs must be set if viaProxy is set to true ' +
        'and must be an array/');
    });
    it('should throw an error if requests.extractClientIPFromHeader ' +
    'is not a string', () => {
        assert.throws(() => {
            requestsConfigAssert({
                viaProxy: true,
                trustedProxyCIDRs: [],
                extractClientIPFromHeader: 1,
            });
        },
        '/config: invalid requests configuration. ' +
        'extractClientIPFromHeader must be set if viaProxy is ' +
        'set to true and must be a string/');
    });
    it('should throw an error if requests.extractClientIPFromHeader ' +
    'is empty', () => {
        assert.throws(() => {
            requestsConfigAssert({
                viaProxy: true,
                trustedProxyCIDRs: [],
                extractClientIPFromHeader: '',
            });
        },
        '/config: invalid requests configuration. ' +
        'extractClientIPFromHeader must be set if viaProxy is ' +
        'set to true and must be a string/');
    });
});
