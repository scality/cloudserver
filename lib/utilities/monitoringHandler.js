const { errors } = require('arsenal');
const client = require('prom-client');
const { config } = require('../Config');

const collectDefaultMetrics = client.collectDefaultMetrics;
const numberOfBuckets = new client.Gauge({
    name: 's3_cloudserver_buckets_count',
    help: 'Total number of buckets',
});
const numberOfObjects = new client.Gauge({
    name: 's3_cloudserver_objects_count',
    help: 'Total number of objects',
});
const lastReportTimestamp = new client.Gauge({
    name: 's3_cloudserver_last_report_timestamp',
    help: 'Timestamp of last object/bucket count report',
});
const numberOfIngestedObjects = new client.Gauge({
    name: 's3_cloudserver_ingested_objects_count',
    help: 'Total number of out of band objects ingested',
});
const dataIngested = new client.Gauge({
    name: 's3_cloudserver_ingested_bytes',
    help: 'Cumulative size of data ingested in bytes',
});
const dataDiskAvailable = new client.Gauge({
    name: 's3_cloudserver_disk_available_bytes',
    help: 'Available data disk storage in bytes',
});
const dataDiskFree = new client.Gauge({
    name: 's3_cloudserver_disk_free_bytes',
    help: 'Free data disk storage in bytes',
});
const dataDiskTotal = new client.Gauge({
    name: 's3_cloudserver_disk_total_bytes',
    help: 'Total data disk storage in bytes',
});

const labelNames = ['method', 'action', 'code'];
const httpRequestsTotal = new client.Counter({
    labelNames,
    name: 's3_cloudserver_http_requests_total',
    help: 'Total number of cloudserver HTTP requests',
});
const httpRequestDurationSeconds = new client.Histogram({
    name: 's3_cloudserver_http_request_duration_seconds',
    help: 'Duration of HTTP requests in seconds',
    labelNames,
    buckets: [0.0001, 0.005, 0.015, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5], // buckets for response time from 0.1ms to 500ms
});
const httpActiveRequests = new client.Gauge({
    name: 's3_cloudserver_http_active_requests',
    help: 'Number of cloudserver HTTP in-flight requests',
});

const httpRequestSizeBytes = new client.Summary({
    labelNames,
    name: 's3_cloudserver_http_request_size_bytes',
    help: 'Cloudserver HTTP request sizes in bytes',
});

const httpResponseSizeBytes = new client.Summary({
    labelNames,
    name: 's3_cloudserver_http_response_size_bytes',
    help: 'Cloudserver HTTP response sizes in bytes',
});

let quotaEvaluationDuration;
let utilizationMetricsRetrievalDuration;
let utilizationServiceAvailable;
let numberOfBucketQuota;
let numberOfAccountQuota;
let requestWithQuotaMetricsUnavailable;

if (config.isQuotaEnabled) {
    quotaEvaluationDuration = new client.Histogram({
        name: 's3_cloudserver_quota_evaluation_duration_seconds',
        help: 'Duration of the quota evaluation operation',
        labelNames: ['action', 'code', 'type'],
        buckets: [0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.5, 1],
    });

    utilizationMetricsRetrievalDuration = new client.Histogram({
        name: 's3_cloudserver_quota_metrics_retrieval_duration_seconds',
        help: 'Duration of the utilization metrics retrieval operation',
        labelNames: ['code', 'class'],
        buckets: [0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.5],
    });

    utilizationServiceAvailable = new client.Gauge({
        name: 's3_cloudserver_quota_utilization_service_available',
        help: 'Availability of the utilization service',
    });

    numberOfBucketQuota = new client.Gauge({
        name: 's3_cloudserver_quota_buckets_count',
        help: 'Total number of buckets quota',
    });

    numberOfAccountQuota = new client.Gauge({
        name: 's3_cloudserver_quota_accounts_count',
        help: 'Total number of account quota',
    });

    requestWithQuotaMetricsUnavailable = new client.Counter({
        name: 's3_cloudserver_quota_request_with_metrics_unavailable',
        help: 'Total number of requests with quota metrics unavailable',
    });
}

// Lifecycle duration metric, to track the completion of restore.
// This metric is used to track the time it takes to complete the lifecycle operation (restore).
// NOTE : this metric is the same as the one defined in Backbeat, and must keep the same name,
// labels and buckets.
const lifecycleDuration = new client.Histogram({
    name: 's3_lifecycle_duration_seconds',
    help: 'Duration of the lifecycle operation, calculated from the theoretical date to the end ' +
        'of the operation',
    labelNames: ['type', 'location'],
    buckets: [0.2, 1, 5, 30, 120, 600, 3600, 4 * 3600, 8 * 3600, 16 * 3600, 24 * 3600],
});

function promMetrics(method, bucketName, code, action,
    newByteLength, oldByteLength, isVersionedObj,
    numOfObjectsRemoved, ingestSize) {
    let bytes;

    switch (action) {
    case 'putObject':
    case 'copyObject':
    case 'putObjectPart':
        if (code === '200') {
            bytes = newByteLength - (isVersionedObj ? 0 : oldByteLength);
            httpRequestSizeBytes
                .labels(method, action, code)
                .observe(newByteLength);
            dataDiskAvailable.dec(bytes);
            dataDiskFree.dec(bytes);
            if (ingestSize) {
                numberOfIngestedObjects.inc();
                dataIngested.inc(ingestSize);
            }
            numberOfObjects.inc();
        }
        break;
    case 'createBucket':
        if (code === '200') {
            numberOfBuckets.inc();
        }
        break;
    case 'getObject':
        if (code === '200') {
            httpResponseSizeBytes
                .labels(method, action, code)
                .observe(newByteLength);
        }
        break;
    case 'deleteBucket':
    case 'deleteBucketWebsite':
        if (code === '200' || code === '204') {
            numberOfBuckets.dec();
        }
        break;
    case 'deleteObject':
    case 'abortMultipartUpload':
    case 'multiObjectDelete':
        if (code === '200') {
            dataDiskAvailable.inc(newByteLength);
            dataDiskFree.inc(newByteLength);
            const objs = numOfObjectsRemoved || 1;
            numberOfObjects.dec(objs);
            if (ingestSize) {
                numberOfIngestedObjects.dec(objs);
                dataIngested.dec(ingestSize);
            }
        }
        break;
    default:
        break;
    }
}

function crrCacheToProm(crrResults) {
    if (crrResults) {
        lastReportTimestamp.setToCurrentTime();
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
}

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
    const promMetrics = await client.register.metrics();
    const contentLen = Buffer.byteLength(promMetrics, 'utf8');
    res.writeHead(200, {
        'Content-Length': contentLen,
        'Content-Type': client.register.contentType,
    });
    res.end(promMetrics);
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
                return log.end().warn('monitoring error', { err: error });
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
    return routeHandler(req, res, monitoringEndHandler);
}

module.exports = {
    monitoringHandler,
    client,
    collectDefaultMetrics,
    promMetrics,
    crrCacheToProm,
    httpRequestDurationSeconds,
    httpRequestsTotal,
    httpActiveRequests,
    lifecycleDuration,
    quotaEvaluationDuration,
    utilizationMetricsRetrievalDuration,
    utilizationServiceAvailable,
    numberOfBucketQuota,
    requestWithQuotaMetricsUnavailable,
};
