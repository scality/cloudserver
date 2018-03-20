const errors = require('arsenal');
const client = require('prom-client');

function writeResponse(res, error, log, results, cb) {
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


function routeHandler(req, res, log, cb) {
    if (req.method !== 'GET') {
        return cb(errors.BadRequest, []);
    }
    client.collectDefaultMetrics();
    const promMetrics = client.register.metrics();
    const contentLen = Buffer.byteLength(promMetrics, 'utf8');
    res.setHeader('content-length', contentLen);
    res.setHeader('content-type', 'application/json');
    res.end(promMetrics);
    return undefined;
}

/**
 * Checks if client IP address is allowed to make http request to
 * S3 server. Defines function 'montiroingEndHandler', which is
 * called if IP not allowed or passed as callback.
 * @param {object} clientIP - IP address of client
 * @param {object} req - http request object
 * @param {object} res - http response object
 * @param {object} log - werelogs logger instance
 * @return {undefined}
 */
function monitoringHandler(clientIP, req, res, log) {
    function monitoringEndHandler(err, results) {
        writeResponse(res, err, log, results, error => {
            if (error) {
                return log.end().warn('monitoring error', { err: error });
            }
            return log.end();
        });
    }
    if (req.method !== 'GET') {
        return monitoringEndHandler(res, errors.MethodNotAllowed);
    }
    const monitoring = (req.url === '/_/monitoring/metrics');
    if (!monitoring) {
        return monitoringEndHandler(res, errors.MethodNotAllowed);
    }
    return routeHandler(req, res, log, monitoringEndHandler);
}

module.exports = {
    monitoringHandler,
};
