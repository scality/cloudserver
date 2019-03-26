const assert = require('assert');

const {
    createWSAgent,
} = require('../../../lib/management/push');

const proxy = 'http://proxy:3128/';
const logger = { info: () => {} };

function testVariableSet(httpProxy, httpsProxy, allProxy, noProxy) {
    return () => {
        test(`should use ${httpProxy} environment variable`, () => {
            let agent = createWSAgent('https://pushserver', {
                [httpProxy]: 'http://proxy:3128',
            }, logger);
            expect(agent).toEqual(null);

            agent = createWSAgent('http://pushserver', {
                [httpProxy]: proxy,
            }, logger);
            expect(agent.proxy.href).toEqual(proxy);
        });

        test(`should use ${httpsProxy} environment variable`, () => {
            let agent = createWSAgent('http://pushserver', {
                [httpsProxy]: proxy,
            }, logger);
            expect(agent).toEqual(null);

            agent = createWSAgent('https://pushserver', {
                [httpsProxy]: proxy,
            }, logger);
            expect(agent.proxy.href).toEqual(proxy);
        });

        test(`should use ${allProxy} environment variable`, () => {
            let agent = createWSAgent('http://pushserver', {
                [allProxy]: proxy,
            }, logger);
            expect(agent.proxy.href).toEqual(proxy);

            agent = createWSAgent('https://pushserver', {
                [allProxy]: proxy,
            }, logger);
            expect(agent.proxy.href).toEqual(proxy);
        });

        test(`should use ${noProxy} environment variable`, () => {
            let agent = createWSAgent('http://pushserver', {
                [noProxy]: 'pushserver',
            }, logger);
            expect(agent).toEqual(null);

            agent = createWSAgent('http://pushserver', {
                [noProxy]: 'pushserver',
                [httpProxy]: proxy,
            }, logger);
            expect(agent).toEqual(null);

            agent = createWSAgent('http://pushserver', {
                [noProxy]: 'pushserver2',
                [httpProxy]: proxy,
            }, logger);
            expect(agent.proxy.href).toEqual(proxy);
        });
    };
}

describe('Websocket connection agent', () => {
    describe('with no proxy env', () => {
        test('should handle empty proxy environment', () => {
            const agent = createWSAgent('https://pushserver', {}, logger);
            expect(agent).toEqual(null);
        });
    });

    describe('with lowercase proxy env',
        testVariableSet('http_proxy', 'https_proxy', 'all_proxy', 'no_proxy'));

    describe('with uppercase proxy env',
        testVariableSet('HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'NO_PROXY'));
});
