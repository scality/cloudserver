'use strict'; // eslint-disable-line strict
const http = require('http');
const assert = require('assert');

describe('Monitoring - getting metrics', () => {
    const conf = require('../config.json');

    async function query(path, method = 'GET', token = 'report-token-1') {
        return new Promise(resolve => http.request({
            method,
            host: conf.ipAddress,
            path,
            port: 8000,
            headers: { 'x-scal-report-token': token },
        }, () => resolve()).end());
    }

    async function getMetrics() {
        return new Promise(resolve => {
            http.get({ host: conf.ipAddress, path: '/metrics', port: 8002 }, res => {
                assert.strictEqual(res.statusCode, 200);

                const body = [];
                res.on('data', chunk => { body.push(chunk); });
                res.on('end', () => resolve(body.join('')));
            });
        });
    }

    function parseMetric(metrics, name, labels) {
        const labelsString = Object.entries(labels !== undefined ? labels : {
            method: 'GET', code: '200', route: 'healthcheck',
        }).map(e => `${e[0]}="${e[1]}"`).join(',');
        const metric = metrics.match(new RegExp(`^${name}{${labelsString}} (.*)$`, 'm'));
        return metric ? metric[1] : null;
    }

    function parseDuration(metrics, labels) {
        const duration = parseMetric(metrics, 'http_request_duration_seconds_sum', labels);
        return duration ? parseFloat(duration) : 0;
    }

    function parseRequestsCount(metrics, labels) {
        const count = parseMetric(metrics, 'http_requests_total', labels);
        return count ? parseInt(count, 10) : 0;
    }

    it('should return system metrics', async () => {
        const metrics = await getMetrics();
        assert(metrics.includes('\nnodejs_active_handles_total '));
    });

    [
        // Check all methods are reported (on unsupported route)
        ['/_/fooooo',       { method: 'GET', code: '400' }],
        ['/_/fooooo',       { method: 'PUT', code: '400' }],
        ['/_/fooooo',       { method: 'POST', code: '400' }],
        ['/_/fooooo',       { method: 'DELETE', code: '400' }],

        // S3/api routes
        ['/',               { method: 'GET', code: '403', route: 'serviceGet' }],
        ['/foo',            { method: 'GET', code: '404', route: 'bucketGet' }],
        ['/foo/bar',        { method: 'GET', code: '404', route: 'objectGet' }],

        // Internal handlers
        ['/_/healthcheck',  { method: 'GET', code: '200', route: 'healthcheck' }],
        ['/_/healthcheck/deep',
                            { method: 'GET', code: '200', route: 'deepHealthcheck' }],
        ['/_/report',       { method: 'GET', code: '200', route: 'report' }],
        ['/_/backbeat',     { method: 'GET', code: '405', route: 'routeBackbeat' }],
        ['/_/metadata',     { method: 'GET', code: '403', route: 'routeMetadata' }],
        ['/_/workflow-engine-operator',
                            { method: 'GET', code: '405', route: 'routeWorkflowEngineOperator' }],
    ].forEach(([path, labels]) => {
        it(`should count http ${labels.method} requests metrics on ${path}`, async () => {
            const count = parseRequestsCount(await getMetrics(), labels);
            for (let i = 1; i <= 3; i++) { /* eslint no-await-in-loop: "off" */
                await query(path, labels.method);

                const c = parseRequestsCount(await getMetrics(), labels);
                assert.strictEqual(c - count, i);
            }
        });
    });

    it('should measure http requests duration metrics', async () => {
        const initialDuration = parseDuration(await getMetrics());
        let previousDuration = initialDuration;
        for (let i = 0; i < 1000; i++) { /* eslint no-await-in-loop: "off" */
            await query('/_/healthcheck');

            const duration = parseDuration(await getMetrics());
            assert(duration >= previousDuration); // May be equal, if host is too fast...

            // Early exit as soon as we are sure it increases somewhat. We don't expect to reach the
            // end of the main loop (i = 1000)
            if (i > 10 && duration > initialDuration) {
                break;
            }
            previousDuration = duration;
        }
        assert(previousDuration > initialDuration); // Expect it will increase, over a few calls...
    });
});
