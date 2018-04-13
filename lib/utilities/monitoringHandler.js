const errors = require('arsenal');
const client = require('prom-client');

const totalBucketNumber = new client.Counter({
    name: 'bucket_number_total',
    help: 'Total number of buckets'
});
const currentBucketNumber = new client.Gauge({ 
    name: 'bucket_number_current', 
    help: 'Current number of buckets' 
});
const currentLifecycleBucketNumber = new client.Gauge({ 
    name: 'bucket_lifecycle_number_current', 
    help: 'Current number of lifecycle buckets' 
});
const totalObjectNumber = new client.Counter({
    name: 'object_number_total',
    help: 'Total number of objects'
});
const currentObjectNumber = new client.Gauge({ 
    name: 'object_number_current', 
    help: 'Current number of objects' 
});
const putRequest = new client.Histogram({
    name: 'put_request',
    help: 'Number of PUT requests',
    labelNames: ['status_code'],
    buckets: [0.1, 5, 15, 50, 100, 500]
});

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
    //remove the below line
    console.log(client.collectDefaultMetrics.metricsList);
    const promMetrics = client.register.metrics();
    const contentLen = Buffer.byteLength(promMetrics, 'utf8');
    res.setHeader('content-length', contentLen);
    res.setHeader('content-type', client.register.contentType);
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
    totalBucketNumber,
    currentBucketNumber,
    currentLifecycleBucketNumber,
    totalObjectNumber,
    currentObjectNumber,
    putRequest
};
