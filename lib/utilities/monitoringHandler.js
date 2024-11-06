const { errors } = require('arsenal');
const client = require('prom-client');

const collectDefaultMetrics = client.collectDefaultMetrics;
const aggregatorRegistry = new client.AggregatorRegistry();

function writeResponse(res, error, results, cb) {
    let statusCode = 200;
    if (error) {
        if (Number.isInteger(error.code)) {
            statusCode = error.code;
        } else {
            statusCode = 500;
        }
    }
    res.writeHead(statusCode, { 'Content-Type': 'application/json' });
    res.write(JSON.stringify(results));
    res.end(() => {
        cb(error, results);
    });
}

async function routeHandler(req, res, cb) {
    if (req.method !== 'GET') {
        return cb(errors.BadRequest, []);
    }
    let metrics;
    try {
        // Catch timeout on IPC between worker and primary
        // prom-client has a 5s hardcoded timeout
        metrics = await aggregatorRegistry.clusterMetrics();
    } catch (err) {
        return cb(err, { message: err.toString() });
    }

    const contentLen = Buffer.byteLength(metrics, 'utf8');
    res.writeHead(200, {
        'Content-Length': contentLen,
        'Content-Type': aggregatorRegistry.contentType,
    });
    res.end(metrics);
    return undefined;
}

/**
 * Checks if client IP address is allowed to make http request to
 * S3 server. Defines function 'montiroingEndHandler', which is
 * called if IP not allowed or passed as callback.
 * @param {string | undefined} clientIP - IP address of client
 * @param {http.IncomingMessage} req - http request object
 * @param {http.ServerResponse} res - http response object
 * @param {RequestLogger} log - werelogs logger instance
 * @return {void}
 */
function monitoringHandler(clientIP, req, res, log) {
    function monitoringEndHandler(err, results) {
        writeResponse(res, err, results, error => {
            if (error) {
                return log.end().warn('monitoring error', { err: {
                    ...error,
                    // For ArsenalError message is in description
                    message: error.description || error.message,
                } });
            }
            return log.end();
        });
    }
    if (req.method !== 'GET') {
        return monitoringEndHandler(errors.MethodNotAllowed, []);
    }
    if (req.url !== '/metrics') {
        return monitoringEndHandler(errors.MethodNotAllowed, []);
    }
    return this.routeHandler(req, res, monitoringEndHandler);
}

module.exports = {
    client,
    collectDefaultMetrics,
    monitoringHandler,
    routeHandler,
};
