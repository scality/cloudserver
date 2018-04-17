const errors = require('arsenal');
const client = require('prom-client');

const collectDefaultMetrics = client.collectDefaultMetrics;
const totalBucketNumber = new client.Counter({
    name: 'cloud_server_bucket_number_total',
    help: 'Total number of buckets created by Zenko(post deployment)',
});
const currentBucketNumber = new client.Gauge({
    name: 'cloud_server_bucket_number_current',
    help: 'Current number of buckets created by Zenko(post deployment)',
});
const currentLifecycleBucketNumber = new client.Gauge({
    name: 'cloud_server_bucket_lifecycle_number_current',
    help: 'Current number of lifecycle buckets created by Zenko',
});
const totalObjectNumber = new client.Counter({
    name: 'cloud_server_object_number_total',
    help: 'Total number of objects uploaded by Zenko(post deployment)',
});
const currentObjectNumber = new client.Gauge({ 
    name: 'cloud_server_object_number_current', 
    help: 'Current number of objects uploaded by Zenko(post deployment)',
});
const getRequest = new client.Counter({
    name: 'cloud_server_get_request',
    help: 'Number of GET requests made through Zenko(post deployment)',
});
const putRequest = new client.Counter({
    name: 'cloud_server_put_request',
    help: 'Number of PUT requests made through Zenko(post deployment)',
});
const deleteRequest = new client.Counter({
    name: 'cloud_server_delete_request',
    help: 'Number of DELETE requests made through Zenko(post deployment)',
});
const headRequest = new client.Counter({
    name: 'cloud_server_head_request',
    help: 'Number of HEAD requests made through Zenko(post deployment)',
});

const bytesUploaded = new client.Counter({
    name: 'cloud_server_bytes_uploaded',
    help: 'Number of bytes uploaded made through Zenko(post deployment)',
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
    client,
    collectDefaultMetrics,
    totalBucketNumber,
    currentBucketNumber,
    currentLifecycleBucketNumber,
    totalObjectNumber,
    currentObjectNumber,
    getRequest,
    bytesUploaded,
    putRequest,
    deleteRequest,
    headRequest,
};
