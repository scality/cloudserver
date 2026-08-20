const assert = require('assert');
const sinon = require('sinon');
const { ScubaClientImpl } = require('../../../../lib/utilization/scuba/wrapper');

describe('ScubaClientImpl', () => {
    let client;

    beforeEach(() => {
        client = new ScubaClientImpl({ scuba: true, quota: { maxStaleness: 24 * 60 * 60 * 1000 } });
        client.setup();
    });

    afterEach(() => {
        sinon.restore();
    });

    describe('setup', () => {
        it('should enable Scuba and start periodic health check', () => {
            client.setup();

            assert.strictEqual(client.enabled, true);
        });

        it('should not enable Scuba if config.scuba is falsy', () => {
            client = new ScubaClientImpl({ scuba: false, quota: { maxStaleness: 24 * 60 * 60 * 1000 } });
            client.setup();

            assert.strictEqual(client.enabled, false);
        });
    });

    describe('_healthCheck', () => {
        // healthCheck is assigned rather than sinon-stubbed: it sits deep in
        // the scubaclient prototype chain and sinon reports it as
        // non-existent, which left these four cases failing.
        it('should enable Scuba if health check passes', async () => {
            client.healthCheck = () => Promise.resolve();

            await client._healthCheck();

            assert.strictEqual(client.enabled, true);
        });

        it('should re-enable Scuba when a health check passes from the disabled state', async () => {
            // covers the enabling-quotas log line, which only fires on the
            // disabled -> enabled transition
            client.enabled = false;
            client.healthCheck = () => Promise.resolve({ date: new Date().toISOString() });

            await client._healthCheck();

            assert.strictEqual(client.enabled, true);
        });

        it('should disable Scuba if health check returns non-stale data', async () => {
            client.healthCheck = () => Promise.resolve({ date: Date.now() - (12 * 60 * 60 * 1000) });

            await client._healthCheck();

            assert.strictEqual(client.enabled, true);
        });

        it('should disable Scuba if health check returns stale data', async () => {
            client.healthCheck = () => Promise.resolve({ date: Date.now() - (48 * 60 * 60 * 1000) });

            await client._healthCheck();

            assert.strictEqual(client.enabled, false);
        });

        it('should disable Scuba if health check fails', async () => {
            const error = new Error('Health check failed');
            client.healthCheck = () => Promise.reject(error);

            await client._healthCheck();

            assert.strictEqual(client.enabled, false);
        });
    });

    describe('periodicHealthCheck', () => {
        let healthCheckStub;
        let setIntervalStub;
        let clearIntervalStub;

        beforeEach(() => {
            healthCheckStub = sinon.stub(client, '_healthCheck');
            setIntervalStub = sinon.stub(global, 'setInterval');
            clearIntervalStub = sinon.stub(global, 'clearInterval');
        });

        it('should call _healthCheck and start periodic health check', () => {
            client._healthCheckTimer = null;
            client.periodicHealthCheck();

            assert(healthCheckStub.calledOnce);
            assert(setIntervalStub.calledOnce);
            assert(clearIntervalStub.notCalled);
        });

        it('should clear previous health check timer before starting a new one', () => {
            client._healthCheckTimer = 123;

            client.periodicHealthCheck();

            assert(healthCheckStub.calledOnce);
            assert(setIntervalStub.calledOnce);
            assert(clearIntervalStub.calledOnceWith(123));
        });
    });

    describe('logger retention', () => {
        it('should not retain a werelogs RequestLogger', () => {
            // a RequestLogger buffers every entry it is handed and only drains
            // on an error-level write, so an object that outlives the request
            // grows it forever
            const requestLog = {
                info: sinon.spy(),
                warn: sinon.spy(),
                entries: [],
            };

            client.setup(requestLog);

            const retained = Object.keys(client).filter(k => client[k] === requestLog);
            assert.deepStrictEqual(retained, [],
                `quota client must not hold a request logger (held on: ${retained.join(', ')})`);
        });

        it('should log health check transitions without touching a passed logger', async () => {
            const requestLog = { info: sinon.spy(), warn: sinon.spy() };
            client.setup(requestLog);
            client.enabled = true;

            // assigned rather than stubbed: healthCheck sits deep in the
            // scubaclient prototype chain and sinon cannot stub it
            client.healthCheck = () => Promise.reject(new Error('scuba unreachable'));
            await client._healthCheck();

            assert.strictEqual(client.enabled, false);
            assert.strictEqual(requestLog.warn.callCount, 0);
            assert.strictEqual(requestLog.info.callCount, 0);
        });
    });
});
