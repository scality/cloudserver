const http = require('http');
const assert = require('assert');

/**
 * Get the metrics cloudserver exposes on its metrics port
 * @param {string} host - host cloudserver runs on
 * @param {number} [port] - metrics port of cloudserver
 * @returns {Promise<string>} the metrics, in the Prometheus text format
 */
function getMetrics(host, port = 8002) {
    return new Promise((resolve, reject) => {
        http.get({ host, path: '/metrics', port }, res => {
            assert.strictEqual(res.statusCode, 200);

            const body = [];
            res.on('data', chunk => {
                body.push(chunk);
            });
            res.on('end', () => resolve(body.join('')));
        }).on('error', reject);
    });
}

/**
 * Value of one series of a metric
 * @param {string} metrics - metrics in the Prometheus text format
 * @param {string} name - name of the metric
 * @param {object} labels - labels of the series, in the order they are printed
 * @returns {string|null} the value, null if the series is not in the metrics
 */
function parseMetric(metrics, name, labels) {
    const labelsString = Object.entries(labels)
        .map(e => `${e[0]}="${e[1]}"`)
        .join(',');
    const metric = metrics.match(new RegExp(`^${name}{${labelsString}} (.*)$`, 'm'));
    return metric ? metric[1] : null;
}

module.exports = { getMetrics, parseMetric };
