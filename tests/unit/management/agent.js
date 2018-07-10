const assert = require('assert');

const {
    createWSAgent,
} = require('../../../lib/management/push');

const proxy = 'http://proxy:3128/';
const logger = { info: () => {} };

function testVariableSet(httpProxy, httpsProxy, allProxy, noProxy) {
    return () => {
        it(`should use ${httpProxy} environment variable`, () => {
            let agent = createWSAgent('https://pushserver', {
                [httpProxy]: 'http://proxy:3128',
            }, logger);
            assert.equal(agent, null);

            agent = createWSAgent('http://pushserver', {
                [httpProxy]: proxy,
            }, logger);
            assert.equal(agent.proxy.href, proxy);
        });

        it(`should use ${httpsProxy} environment variable`, () => {
            let agent = createWSAgent('http://pushserver', {
                [httpsProxy]: proxy,
            }, logger);
            assert.equal(agent, null);

            agent = createWSAgent('https://pushserver', {
                [httpsProxy]: proxy,
            }, logger);
            assert.equal(agent.proxy.href, proxy);
        });

        it(`should use ${allProxy} environment variable`, () => {
            let agent = createWSAgent('http://pushserver', {
                [allProxy]: proxy,
            }, logger);
            assert.equal(agent.proxy.href, proxy);

            agent = createWSAgent('https://pushserver', {
                [allProxy]: proxy,
            }, logger);
            assert.equal(agent.proxy.href, proxy);
        });

        it(`should use ${noProxy} environment variable`, () => {
            let agent = createWSAgent('http://pushserver', {
                [noProxy]: 'pushserver',
            }, logger);
            assert.equal(agent, null);

            agent = createWSAgent('http://pushserver', {
                [noProxy]: 'pushserver',
                [httpProxy]: proxy,
            }, logger);
            assert.equal(agent, null);

            agent = createWSAgent('http://pushserver', {
                [noProxy]: 'pushserver2',
                [httpProxy]: proxy,
            }, logger);
            assert.equal(agent.proxy.href, proxy);
        });
    };
}

describe('Websocket connection agent', () => {
    describe('with no proxy env', () => {
        it('should handle empty proxy environment', () => {
            const agent = createWSAgent('https://pushserver', {}, logger);
            assert.equal(agent, null);
        });
    });

    describe('with lowercase proxy env',
        testVariableSet('http_proxy', 'https_proxy', 'all_proxy', 'no_proxy'));

    describe('with uppercase proxy env',
        testVariableSet('HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'NO_PROXY'));
});
