const assert = require('assert');
const sinon = require('sinon');
const { ScubaClientImpl } = require('../../../lib/scuba/wrapper');

describe('ScubaClientImpl', () => {
    let client;
    let log;

    beforeEach(() => {
        client = new ScubaClientImpl({ scuba: true });
        log = {
            info: sinon.spy(),
            warn: sinon.spy(),
        };
        client.setup(log);
    });

    afterEach(() => {
        sinon.restore();
    });

    describe('setup', () => {
        it('should enable Scuba and start periodic health check', () => {
            client.setup(log);

            assert.strictEqual(client.enabled, true);
        });

        it('should not enable Scuba if config.scuba is falsy', () => {
            client = new ScubaClientImpl({ scuba: false });
            client.setup(log);

            assert.strictEqual(client.enabled, false);
        });
    });

    describe('_healthCheck', () => {
        it('should enable Scuba if health check passes', async () => {
            sinon.stub(client, 'healthCheck').resolves();

            await client._healthCheck();

            assert.strictEqual(client.enabled, true);
        });

        it('should disable Scuba if health check fails', async () => {
            const error = new Error('Health check failed');
            sinon.stub(client, 'healthCheck').rejects(error);

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
});
