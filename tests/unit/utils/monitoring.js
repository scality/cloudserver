/* eslint-disable no-unused-expressions */
const assert = require('assert');
const promclient = require('prom-client');
const sinon = require('sinon');

const monitoring = require('../../../lib/utilities/monitoringHandler');
const metrics = require('../../../lib/utilities/metrics');

describe('Monitoring: endpoint', () => {
    const sandbox = sinon.createSandbox();
    const res = {
        writeHead(/* result, headers */) { return this; },
        write(/* body */) { return this; },
        end(/* body */) {},
    };
    monitoring.collectDefaultMetrics();

    // adding a fake to route handler because `routeHandler` is implemented to
    // gather aggregated metrics, the unit tests here are executed in master and
    // the prom-client aggregator registry will not collect metrics from master
    // TODO: refactor unit tests to not check routes/response & only check
    // for list of custom metrics & system metrics, move response code tests to
    // functional tests.
    async function fakeRouteHandler(req, res) {
        const metrics = await promclient.register.metrics();
        const contentLen = Buffer.byteLength(metrics, 'utf8');
        res.writeHead(200, {
            'Content-Length': contentLen,
            'Content-Type': promclient.register.contentType,
        });
        res.end(metrics);
        return undefined;
    }
    beforeEach(() => {
        sandbox.spy(res);
        sandbox.spy(promclient.register, 'metrics');
        sandbox.replace(monitoring, 'routeHandler', fakeRouteHandler);
    });

    afterEach(() => {
        sandbox.restore();
    });

    async function fetchMetrics(req, res) {
        await new Promise(resolve => monitoring.monitoringHandler(null, req,
            {
                ...res,
                end: (...body) => {
                    res.end(...body);
                    resolve();
                },
            }, null));
    }

    it('should return an error if method is not GET', async () => {
        await fetchMetrics({ method: 'PUT', url: '/metrics' }, res);
        assert(res.writeHead.calledOnceWith(405));
        assert(res.end.calledOnce);
    });

    it('should return an error if path is not /metrics', async () => {
        await fetchMetrics({ method: 'GET', url: '/foo' }, res);
        assert(res.writeHead.calledOnceWith(405));
        assert(res.end.calledOnce);
    });

    it('should return some metrics', async () => {
        await fetchMetrics({ method: 'GET', url: '/metrics' }, res);
        assert(res.writeHead.calledOnceWith(200));
        assert(res.end.calledOnce);

        // Check that some "system" metrics is present
        assert(res.end.args[0][0].includes('\nnodejs_active_handles_total '));
    });

    it('should have http duration histogram metrics', async () => {
        await fetchMetrics({ method: 'GET', url: '/metrics' }, res);
        assert(res.writeHead.calledOnceWith(200));
        assert(res.end.args[0][0].includes('\n# TYPE s3_cloudserver_http_request_duration_seconds histogram'));
    });

    it('should have http requests counter metrics', async () => {
        await fetchMetrics({ method: 'GET', url: '/metrics' }, res);
        assert(res.writeHead.calledOnceWith(200));
        assert(res.end.args[0][0].includes('\n# TYPE s3_cloudserver_http_requests_total counter'));
    });

    it('should have http active requests gauge metrics', async () => {
        await fetchMetrics({ method: 'GET', url: '/metrics' }, res);
        assert(res.writeHead.calledOnceWith(200));
        assert(res.end.args[0][0].includes('\n# TYPE s3_cloudserver_http_active_requests gauge'));
    });

    it('should have http requests size metrics', async () => {
        await fetchMetrics({ method: 'GET', url: '/metrics' }, res);
        assert(res.writeHead.calledOnceWith(200));
        assert(res.end.args[0][0].includes('\n# TYPE s3_cloudserver_http_request_size_bytes summary'));
    });

    it('should have http response size metrics', async () => {
        await fetchMetrics({ method: 'GET', url: '/metrics' }, res);
        assert(res.writeHead.calledOnceWith(200));
        assert(res.end.args[0][0].includes('\n# TYPE s3_cloudserver_http_response_size_bytes summary'));
    });

    function parseMetric(metrics, name, labels) {
        const labelsString = Object.entries(labels).map(e => `${e[0]}="${e[1]}"`).join(',');
        const metric = metrics.match(new RegExp(`^${name}{${labelsString}} (.*)$`, 'm'));
        return metric ? metric[1] : null;
    }

    function parseHttpRequestSize(metrics, action = 'putObject') {
        const value = parseMetric(metrics, 's3_cloudserver_http_request_size_bytes_sum',
            { method: 'PUT', action, code: '200' });
        return value ? parseInt(value, 10) : 0;
    }

    function parseHttpResponseSize(metrics, action = 'getObject') {
        const value = parseMetric(metrics, 's3_cloudserver_http_response_size_bytes_sum',
            { method: 'GET', action, code: '200' });
        return value ? parseInt(value, 10) : 0;
    }

    it('should measure http requests size on putObject', async () => {
        await fetchMetrics({ method: 'GET', url: '/metrics' }, res);
        const requestSize = parseHttpRequestSize(res.end.args[0][0]);

        metrics.promMetrics('PUT', 'stuff', '200',
            'putObject', 2357, 3572, false, null, 5723);

        await fetchMetrics({ method: 'GET', url: '/metrics' }, res);
        assert(parseHttpRequestSize(res.end.args[1][0]) === requestSize + 2357);
    });

    it('should measure http response size on getObject', async () => {
        await fetchMetrics({ method: 'GET', url: '/metrics' }, res);
        const responseSize = parseHttpResponseSize(res.end.args[0][0]);

        metrics.promMetrics('GET', 'stuff', '200',
            'getObject', 7532);

        await fetchMetrics({ method: 'GET', url: '/metrics' }, res);
        assert(parseHttpResponseSize(res.end.args[1][0]) === responseSize + 7532);
    });
});
