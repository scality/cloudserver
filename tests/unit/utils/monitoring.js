const assert = require('assert');
const promclient = require('prom-client');
const sinon = require('sinon');

const monitoring = require('../../../lib/utilities/monitoringHandler');

describe('Monitoring: endpoint', () => {
    const sandbox = sinon.createSandbox();
    const res = {
        writeHead(/* result, headers */) { return this; },
        write(/* body */) { return this; },
        end(/* body */) {},
    };
    monitoring.collectDefaultMetrics();

    beforeEach(() => {
        sandbox.spy(res);
        sandbox.spy(promclient.register, 'metrics');
    });

    afterEach(() => {
        sandbox.restore();
    });

    async function fetchMetrics(req, res) {
        await new Promise(resolve => monitoring.monitoringHandler(null, req, {
            ...res, end: (...body) => { res.end(...body); resolve(); }
        }, null));
    }

    it('it should return an error is method is not GET', async () => {
        await fetchMetrics({ method: 'PUT', url: '/metrics' }, res);
        assert(res.writeHead.calledOnceWith(405));
        assert(res.end.calledOnce);
    });

    it('it should return an error is path is not /metrics', async () => {
        await fetchMetrics({ method: 'GET', url: '/foo' }, res);
        assert(res.writeHead.calledOnceWith(405));
        assert(res.end.calledOnce);
    });

    it('it should return some metrics', async () => {
        await fetchMetrics({ method: 'GET', url: '/metrics' }, res);
        assert(res.writeHead.calledOnceWith(200));
        assert(res.end.calledOnce);

        // Check that some "system" metrics is present
        assert(res.end.args[0][0].includes('\nnodejs_active_handles_total '));
    });

    it('it should have http duration histogram metrics', async () => {
        await fetchMetrics({ method: 'GET', url: '/metrics' }, res);
        assert(res.writeHead.calledOnceWith(200) || !res.writeHead.called);
        assert(res.end.args[0][0].includes('\n# TYPE http_request_duration_seconds histogram'));
    });

    it('it should have http requests counter metrics', async () => {
        await fetchMetrics({ method: 'GET', url: '/metrics' }, res);
        assert(res.writeHead.calledOnceWith(200) || !res.writeHead.called);
        assert(res.end.args[0][0].includes('\n# TYPE http_requests_total counter'));
    });

    it('it should have http active requests gauge metrics', async () => {
        await fetchMetrics({ method: 'GET', url: '/metrics' }, res);
        assert(res.writeHead.calledOnceWith(200) || !res.writeHead.called);
        assert(res.end.args[0][0].includes('\n# TYPE http_active_requests gauge'));
    });
});
