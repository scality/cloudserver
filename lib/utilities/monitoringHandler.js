const errors = require('arsenal');
const client = require('prom-client');

const collectDefaultMetrics = client.collectDefaultMetrics;
let crrStatsPulled = false;
const numberOfBuckets = new client.Gauge({
    name: 'cloud_server_number_of_buckets',
    help: 'Total number of buckets',
});
const numberOfObjects = new client.Gauge({
    name: 'cloud_server_number_of_objects',
    help: 'Total number of objects',
});
const dataDiskAvailable = new client.Gauge({
    name: 'cloud_server_data_disk_available',
    help: 'Available data disk storage in bytes',
});
const dataDiskFree = new client.Gauge({
    name: 'cloud_server_data_disk_free',
    help: 'Free data disk storage in bytes',
});
const dataDiskTotal = new client.Gauge({
    name: 'cloud_server_data_disk_total',
    help: 'Total data disk storage in bytes',
});

const labelNames = ['method', 'service', 'code'];
const httpRequestsTotal = new client.Counter({
    labelNames,
    name: 'cloud_server_http_requests_total',
    help: 'Total number of HTTP requests',
});

const httpRequestSizeBytes = new client.Summary({
    labelNames,
    name: 'cloud_server_http_request_size_bytes',
    help: 'The HTTP request sizes in bytes.',
});

const httpResponseSizeBytes = new client.Summary({
    labelNames,
    name: 'cloud_server_http_response_size_bytes',
    help: 'The HTTP response sizes in bytes.',
});

function promMetrics(requestType, bucketName, code, typeOfRequest,
    newByteLength, oldByteLength, isVersionedObj,
    numOfObjectsRemoved) {
    httpRequestsTotal.labels(requestType, 'cloud_server', code).inc();
    if ((typeOfRequest === 'putObject' ||
    typeOfRequest === 'copyObject' ||
    typeOfRequest === 'putOjectPart') &&
    code === '200') {
        httpRequestSizeBytes
            .labels(requestType, 'cloud_server', code)
            .observe(newByteLength - (isVersionedObj ? 0 : oldByteLength));
        dataDiskAvailable.dec(newByteLength -
            (isVersionedObj ? 0 : oldByteLength));
        dataDiskFree.dec(newByteLength -
            (isVersionedObj ? 0 : oldByteLength));
        numberOfObjects.inc();
    }
    if (typeOfRequest === 'createBucket' && code === '200') {
        numberOfBuckets.inc();
    }
    if (typeOfRequest === 'getObject' && code === '200') {
        httpResponseSizeBytes
            .labels(requestType, 'cloud_server', code)
            .observe(newByteLength);
    }
    if ((typeOfRequest === 'deleteBucket' ||
    typeOfRequest === 'deleteBucketWebsite')
    && code === '200') {
        numberOfBuckets.dec();
    }
    if ((typeOfRequest === 'deleteObject' ||
    typeOfRequest === 'abortMultipartUpload' ||
    typeOfRequest === 'multiObjectDelete')
    && code === '200') {
        dataDiskAvailable.inc(newByteLength);
        dataDiskFree.inc(newByteLength);
        if (numOfObjectsRemoved) {
            numberOfObjects.dec(numOfObjectsRemoved);
        } else {
            numberOfObjects.dec();
        }
    }
}

function crrCacheToProm(crrResults) {
    if (!crrStatsPulled && crrResults) {
        if (crrResults.getObjectCount) {
            numberOfBuckets.set(crrResults.getObjectCount.buckets || 0);
            numberOfObjects.set(crrResults.getObjectCount.objects || 0);
        }
        if (crrResults.getDataDiskUsage) {
            dataDiskAvailable.set(crrResults.getDataDiskUsage.available || 0);
            dataDiskFree.set(crrResults.getDataDiskUsage.free || 0);
            dataDiskTotal.set(crrResults.getDataDiskUsage.total || 0);
        }
    }
    crrStatsPulled = true;
}

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
    promMetrics,
    crrCacheToProm,
};
