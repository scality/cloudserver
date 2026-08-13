const assert = require('assert');
const sinon = require('sinon');
const { ScubaClientImpl } = require('../../../../lib/utilization/scuba/wrapper');
const monitoring = require('../../../../lib/utilities/monitoringHandler');
const { default: ScubaClient } = require('scubaclient');

describe('ScubaClientImpl', () => {
    let client;
    let log;

    beforeEach(() => {
        client = new ScubaClientImpl({ scuba: true, quota: { maxStaleness: 24 * 60 * 60 * 1000 } });
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
            client = new ScubaClientImpl({ scuba: false, quota: { maxStaleness: 24 * 60 * 60 * 1000 } });
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

        it('should disable Scuba if health check returns non-stale data', async () => {
            sinon.stub(client, 'healthCheck').resolves({ date: Date.now() - 12 * 60 * 60 * 1000 });

            await client._healthCheck();

            assert.strictEqual(client.enabled, true);
        });

        it('should disable Scuba if health check returns stale data', async () => {
            sinon.stub(client, 'healthCheck').resolves({ date: Date.now() - 48 * 60 * 60 * 1000 });

            await client._healthCheck();

            assert.strictEqual(client.enabled, false);
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

    describe('getUtilizationMetrics', () => {
        it('should forward the werelogs req_id chain as the X-Scal-Request-Uids header', async () => {
            const metricsStub = sinon.stub(ScubaClient.prototype, 'getLatestMetrics').resolves({ bytesTotal: 0 });
            const reqLog = { getSerializedUids: () => 'req1:req2' };

            const data = await client.getUtilizationMetrics(
                'bucket',
                'k',
                null,
                { action: 'objectPut', inflight: 1 },
                reqLog,
            );

            assert.deepStrictEqual(data, { bytesTotal: 0 });
            const forwardedOptions = metricsStub.getCall(0).args[2];
            assert.strictEqual(forwardedOptions.headers['X-Scal-Request-Uids'], 'req1:req2');
        });

        it('should not set X-Scal-Request-Uids when log lacks getSerializedUids', async () => {
            const metricsStub = sinon.stub(ScubaClient.prototype, 'getLatestMetrics').resolves({});

            await client.getUtilizationMetrics('bucket', 'k', null, { action: 'objectPut', inflight: 1 }, {});

            const forwardedOptions = metricsStub.getCall(0).args[2];
            assert.strictEqual(forwardedOptions, null);
        });

        it('should merge X-Scal-Request-Uids with existing options headers', async () => {
            const metricsStub = sinon.stub(ScubaClient.prototype, 'getLatestMetrics').resolves({ bytesTotal: 0 });
            const reqLog = { getSerializedUids: () => 'req1:req2' };
            const options = { headers: { 'X-Existing': 'v' } };

            await client.getUtilizationMetrics('bucket', 'k', options, { action: 'objectPut', inflight: 1 }, reqLog);

            const forwardedOptions = metricsStub.getCall(0).args[2];
            assert.strictEqual(forwardedOptions.headers['X-Existing'], 'v');
            assert.strictEqual(forwardedOptions.headers['X-Scal-Request-Uids'], 'req1:req2');
        });

        describe('retrieval duration metric', () => {
            let observe;
            let labels;
            let originalMetric;

            beforeEach(() => {
                observe = sinon.spy();
                labels = sinon.stub().returns({ observe });
                originalMetric = monitoring.utilizationMetricsRetrievalDuration;
                monitoring.utilizationMetricsRetrievalDuration = { labels };
            });

            afterEach(() => {
                monitoring.utilizationMetricsRetrievalDuration = originalMetric;
            });

            const observedCode = async error => {
                if (error) {
                    sinon.stub(ScubaClient.prototype, 'getLatestMetrics').rejects(error);
                    await assert.rejects(client.getUtilizationMetrics('bucket', 'k', null, {}, {}));
                } else {
                    sinon.stub(ScubaClient.prototype, 'getLatestMetrics').resolves({});
                    await client.getUtilizationMetrics('bucket', 'k', null, {}, {});
                }
                assert(observe.calledOnce);
                return labels.getCall(0).args[0].code;
            };

            it('should label a successful retrieval with 200', async () => {
                assert.strictEqual(await observedCode(null), 200);
            });

            it('should label an axios error with its response status', async () => {
                const err = new Error('Not Found');
                err.response = { status: 404 };
                err.code = 'ERR_BAD_REQUEST';

                assert.strictEqual(await observedCode(err), 404);
            });

            it('should label a transport failure with the error code', async () => {
                const err = new Error('connect ECONNREFUSED');
                err.code = 'ECONNREFUSED';

                assert.strictEqual(await observedCode(err), 'ECONNREFUSED');
            });

            it('should label an error carrying no status with 500', async () => {
                assert.strictEqual(await observedCode(new Error('boom')), 500);
            });
        });
    });
});
