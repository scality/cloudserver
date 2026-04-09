const fs = require('fs');
const os = require('os');
const logger = require('./logger');

const SERVER_ACCESS_LOG_FORMAT_VERSION = '0';

var serverAccessLogger = undefined;
const hostname = os.hostname();

class ServerAccessLogger {
    constructor(filename, highWaterMarkBytes, retryReopenDelayMS, checkFileRotationIntervalMS) {
        this._filename = filename;
        this._highWaterMarkBytes = highWaterMarkBytes;
        this._retryReopenDelayMS = retryReopenDelayMS;
        this._stat = undefined;
        this._waitingDrain = false;
        this._terminated = false;
        this._reopenStream();

        setInterval(() => { this._checkFileRotated(); }, checkFileRotationIntervalMS);

        process.on('beforeExit', () => {
            this._cleanOldStream(true);
            this._terminated = true;
        });
        process.on('SIGINT', () => {
            this._cleanOldStream(true);
            this._terminated = true;
        });
        process.on('SIGTERM', () => {
            this._cleanOldStream(true);
            this._terminated = true;
        });
        process.on('uncaughtException', () => {
            this._cleanOldStream(true);
            this._terminated = true;
        });
    }

    _checkFileRotated() {
        logger.debug('ServerAccessLogger: check file rotation');

        fs.stat(this._filename, (err, stat) => {
            if (!this._stat) {
                logger.debug('ServerAccessLogger: no active file skip rotation check');
                return;
            }

            if (err) {
                if (err.code === 'ENOENT') {
                    logger.info('ServerAccessLogger: log file doesn\'t exist, creating a new one');
                    this._reopenStream();
                    return;
                }

                logger.error('ServerAccessLogger: failed checkFileRotated: stat error', err);
                return;
            }

            if (stat.ino !== this._stat.ino || stat.dev !== this._stat.dev) {
                logger.info('ServerAccessLogger: log file changed, opening the new one');
                this._reopenStream();
                return;
            }
        });
    }

    _cleanOldStream(end) {
        logger.debug('ServerAccessLogger: cleaning old stream');

        if (!this.stream) {
            return;
        }

        // remove the 'close' handler.
        this.stream.removeAllListeners();
        if (end) {
            this.stream.end();
        }
        this._stat = undefined;
        this.stream = undefined;
        this._waitingDrain = false;
    }

    _reopenStream() {
        fs.open(this._filename, 'a', 0o644, (err, fd) => {
            if (err) {
                logger.error('ServerAccessLogger: failed to reopenStream: open error', err);
                setTimeout(() => { this._reopenStream(); }, this._retryReopenDelayMS);
                return;
            }

            fs.fstat(fd, (err, stat) => {
                if (err || this._terminated) {
                    if (this._terminated) {
                        logger.error('ServerAccessLogger: terminated aborting reopen');
                        fs.close(fd);
                        return;
                    }

                    logger.error('ServerAccessLogger: failed to reopenStream: stat error', err);
                    fs.close(fd);
                    setTimeout(() => { this._reopenStream(); }, this._retryReopenDelayMS);
                    return;
                }

                if (this.stream) {
                    this._cleanOldStream(true);
                }

                this._stat = stat;
                this.stream = fs.createWriteStream(this._filename, {
                    encoding: 'utf-8',
                    flush: true,
                    highWaterMark: this._highWaterMarkBytes,
                    fd,
                });

                this.stream.on('error', err => {
                    logger.error('ServerAccessLogger: stream error', err);
                    // close is called after error so no need to reopen here.
                });

                this.stream.on('close', () => {
                    logger.info('ServerAccessLogger: stream closed');
                    this._cleanOldStream(false);
                    this._reopenStream();
                });
            });
        });
    }

    write(data) {
        if (this._waitingDrain) {
            logger.error('ServerAccessLogger: dropped log entries due to backpressure');
            return;
        }

        if (!this.stream) {
            logger.error('ServerAccessLogger: stream unavailable');
            return;
        }

        if (!this.stream.write(data)) {
            logger.warn('ServerAccessLogger: backpressure buffer full');
            this._waitingDrain = true;
            this.stream.once('drain', () => { this._waitingDrain = false; });
        }
    }
};

function setServerAccessLogger(logger) {
    serverAccessLogger = logger;
}

/**
 * initializeInternalLogRequestQueue - Initialize the queue for internal log request logging
 * @param {object} request - HTTP request object
 * @return {undefined}
 */
function initializeInternalLogRequestQueue(request) {
    if (!request.serverAccessLog) {
        return;
    }
    // eslint-disable-next-line no-param-reassign
    request.serverAccessLog.internalLogRequestQueue = [];
}

/**
 * queueInternalLogRequest - Queue an internal log request entry for later processing
 * @param {object} request - HTTP request object
 * @param {object} entry - Log entry data
 * @return {undefined}
 */
function queueInternalLogRequest(request, entry) {
    if (!request.serverAccessLog || !request.serverAccessLog.internalLogRequestQueue) {
        return;
    }
    request.serverAccessLog.internalLogRequestQueue.push(entry);
}

function getRemoteIPFromRequest(request) {
    let remoteIP = null;
    if (request.headers) {
        // Check for forwarded IP headers (proxy/load balancer scenarios)
        const headerRemoteIP = request.headers['x-forwarded-for'] ||
            request.headers['x-real-ip'] ||
            request.headers['x-client-ip'] ||
            request.headers['cf-connecting-ip']; // Cloudflare

        // x-forwarded-for can contain multiple IPs, take the first one
        if (headerRemoteIP) {
            remoteIP = headerRemoteIP.includes(',') ? headerRemoteIP.split(',')[0].trim() : headerRemoteIP;
        }
    }

    // Fallback to connection remote address if no forwarded headers
    if (!remoteIP) {
        const connIP = (request.connection && request.connection.remoteAddress) ||
            (request.socket && request.socket.remoteAddress) ||
            (request.ip);
        if (connIP) {
            remoteIP = connIP;
        }
    }

    return remoteIP;
}

// eslint-disable-next-line max-len
// https://github.com/awslabs/glue-extensions-for-iceberg/blob/52bdb2908216a85859fd76a45981d0326d016a2f/spark/src/main/scala/software/amazon/glue/s3a/audit/S3LogVerbs.java
// https://github.com/open-io/swift/blob/ff518e9907f74b5a2565973a260f36386b5d5cbf/etc/s3-default.cfg.in#L78
// https://stackoverflow.com/questions/42707878/amazon-s3-logs-operation-definition
const methodToResType = Object.freeze({
    'bucketDelete': 'BUCKET',
    'bucketDeleteCors': 'CORS',
    'bucketDeleteEncryption': 'ENCRYPTION',
    'bucketDeleteWebsite': 'WEBSITE',
    'bucketGet': 'BUCKET',
    'bucketGetACL': 'ACL',
    'bucketGetCors': 'CORS',
    'bucketGetObjectLock': 'OBJECT',
    'bucketGetVersioning': 'VERSIONING',
    'bucketGetWebsite': 'WEBSITE',
    'bucketGetLocation': 'LOCATION',
    'bucketGetEncryption': 'ENCRYPTION',
    'bucketHead': 'BUCKET',
    'bucketPut': 'BUCKET',
    'bucketPutACL': 'ACL',
    'bucketPutCors': 'CORS',
    'bucketPutVersioning': 'VERSIONING',
    'bucketPutTagging': 'TAGGING',
    'bucketDeleteTagging': 'TAGGING',
    'bucketGetTagging': 'TAGGING',
    'bucketPutWebsite': 'WEBSITE',
    'bucketPutReplication': 'REPLICATION',
    'bucketGetReplication': 'REPLICATION',
    'bucketDeleteReplication': 'REPLICATION',
    'bucketDeleteQuota': 'QUOTA',
    'bucketPutLifecycle': 'LIFECYCLE',
    'bucketUpdateQuota': 'QUOTA',
    'bucketGetLifecycle': 'LIFECYCLE',
    'bucketDeleteLifecycle': 'LIFECYCLE',
    'bucketPutPolicy': 'BUCKETPOLICY',
    'bucketGetPolicy': 'BUCKETPOLICY',
    'bucketGetQuota': 'QUOTA',
    'bucketDeletePolicy': 'BUCKETPOLICY',
    'bucketPutObjectLock': 'OBJECT',
    'bucketPutNotification': 'NOTIFICATION',
    'bucketGetNotification': 'NOTIFICATION',
    'bucketPutEncryption': 'ENCRYPTION',
    'bucketPutLogging': 'LOGGING_STATUS',
    'bucketGetLogging': 'LOGGING_STATUS',
    'bucketPutRateLimit': 'RATELIMIT',
    'bucketGetRateLimit': 'RATELIMIT',
    'bucketDeleteRateLimit': 'RATELIMIT',
    'corsPreflight': 'PREFLIGHT',
    'completeMultipartUpload': 'UPLOAD',
    'initiateMultipartUpload': 'UPLOADS',
    'listMultipartUploads': 'UPLOADS',
    'listParts': 'UPLOAD',
    'metadataSearch': 'OBJECT',
    'multiObjectDelete': 'MULTI_OBJECT_DELETE',
    'multipartDelete': 'UPLOAD',
    'objectDelete': 'OBJECT',
    'objectDeleteTagging': 'TAGGING',
    'objectGet': 'OBJECT',
    'objectGetAttributes': 'OBJECT',
    'objectGetACL': 'ACL',
    'objectGetLegalHold': 'LEGALHOLD',
    'objectGetRetention': 'OBJECT_LOCK_RETENTION',
    'objectGetTagging': 'TAGGING',
    'objectCopy': 'COPY',
    'objectHead': 'OBJECT',
    'objectPut': 'OBJECT',
    'objectPutACL': 'ACL',
    'objectPutLegalHold': 'LEGALHOLD',
    'objectPutTagging': 'TAGGING',
    'objectPutPart': 'PART',
    'objectPutCopyPart': 'COPY',
    'objectPutRetention': 'OBJECT_LOCK_RETENTION',
    'objectRestore': 'OBJECT',
    'serviceGet': 'SERVICE', // ListBuckets
    'websiteGet': 'WEBSITE',
    'websiteHead': 'WEBSITE',
});

function getOperation(req) {
    const resourceType = methodToResType[req.apiMethod];

    if (req.serverAccessLog && req.serverAccessLog.backbeat) {
        return `REST.${req.method}.BACKBEAT`;
    }

    // Special handling for copy operations
    if (req.apiMethod === 'objectCopy') {
        return 'REST.COPY.OBJECT';
    }
    if (req.apiMethod === 'objectPutCopyPart') {
        return 'REST.COPY.PART';
    }
    // Special handling for website operations
    if (req.apiMethod === 'websiteGet') {
        return 'WEBSITE.GET.OBJECT';
    }
    if (req.apiMethod === 'websiteHead') {
        return 'WEBSITE.HEAD.OBJECT';
    }

    if (!resourceType) {
        // Only emit a warning if apiMethod is not undefined, meaning request is valid.
        // Otherwise we could get spam by invalid or broken requests.
        // To help catch missing valid operation.
        if (req.apiMethod) {
            process.emitWarning('Unknown apiMethod for server access log', {
                type: 'ServerAccessLogWarning',
                code: 'UNKNOWN_API_METHOD',
                detail: `apiMethod=${req.apiMethod}, method=${req.method}, url=${req.url}`
            });
        }
        return `REST.${req.method}.UNKNOWN`;
    }

    return `REST.${req.method}.${resourceType}`;
}

function getRequester(authInfo) {
    const requester = null;
    if (authInfo) {
        if (authInfo.isRequesterPublicUser && authInfo.isRequesterPublicUser()) {
            return requester; // Unauthenticated requests
        } else if (authInfo.isRequesterAnIAMUser && authInfo.isRequesterAnIAMUser()) {
            // IAM user: include IAM user name and account
            const iamUserName = authInfo.getIAMdisplayName ? authInfo.getIAMdisplayName() : '';
            const accountName = authInfo.getAccountDisplayName ? authInfo.getAccountDisplayName() : '';
            return iamUserName && accountName ? `${iamUserName}:${accountName}` : authInfo.getCanonicalID();
        } else if (authInfo.getCanonicalID) {
            // Regular user: canonical user ID
            return authInfo.getCanonicalID();
        }
    }
    return requester;
}

function getURI(request) {
    let requestURI = null;
    if (request) {
        const method = request.method || 'UNKNOWN';
        const url = request.url || '/';
        const httpVersion = request.httpVersion || '1.1';
        requestURI = `${method} ${url} HTTP/${httpVersion}`;
    }
    return requestURI;
}

const objectSizePutMethods = Object.freeze({
    'objectPut': true,
    'objectPutPart': true,
});

const objectSizeGetMethods = Object.freeze({
    'objectGet': true,
});

function getObjectSize(request, response) {
    // If it is a PUT get the Content-Length from the request, if it is a GET get it from the response.
    if (request && response && objectSizeGetMethods[request.apiMethod]) {
        const len = response.getHeader('Content-Length');
        if (len === undefined || len === null || len === '') {
            return null;
        }

        const numLen = Number(len);
        if (isNaN(numLen)) {
            return null;
        }

        return numLen;
    }

    if (request && objectSizePutMethods[request.apiMethod]) {
        const len = request.headers['content-length'];
        if (len === undefined || len === null || len === '') {
            return null;
        }

        const numLen = Number(len);
        if (isNaN(numLen)) {
            return null;
        }

        return numLen;
    }

    return null;
}

function getBytesSent(res, bytesSent) {
    if (bytesSent !== undefined && bytesSent !== null) {
        return bytesSent;
    }

    if (!res) {
        return null;
    }

    const len = res.getHeader('Content-Length');
    if (len === undefined || len === null) {
        return null;
    }

    return len;
}

function calculateTotalTime(startTime, onFinishEndTime) {
    if (startTime === undefined || startTime === null || onFinishEndTime === undefined || onFinishEndTime === null) {
        return null;
    }

    return ((onFinishEndTime - startTime) / 1_000_000n).toString();
}

function calculateTurnAroundTime(startTurnAroundTime, endTurnAroundTime) {
    if (startTurnAroundTime === undefined || startTurnAroundTime === null
        || endTurnAroundTime === undefined || endTurnAroundTime === null) {
        return null;
    }

    return ((endTurnAroundTime - startTurnAroundTime) / 1_000_000n).toString();
}

function calculateElapsedMS(startTime, onCloseEndTime) {
    if (startTime === undefined || startTime === null || onCloseEndTime === undefined || onCloseEndTime === null) {
        return null;
    }

    return Number(onCloseEndTime - startTime) / 1_000_000;
}

function timestampToDateTime643(unixMS) {
    if (unixMS === undefined || unixMS === null) {
        return null;
    }

    // clickhouse DateTime64(3) expects "seconds.milliseconds" (string type).
    return (unixMS / 1000).toFixed(3);
}

/**
 * Builds a log entry object with common fields
 * @param {object} req - HTTP request object
 * @param {object} params - Server access log parameters from req.serverAccessLog
 * @param {object} options - Variable fields for the log entry
 * @return {object} Log entry object
 */
function buildLogEntry(req, params, options) {
    const authInfo = params.authInfo;

    return {
        // Werelog fields
        // logs.access_logs_ingest.timestamp in Clickhouse is of type DateTime so it expects seconds as an integer.
        time: Math.floor(Date.now() / 1000),
        hostname,
        pid: process.pid,

        // Analytics
        action: params.analyticsAction ?? undefined,
        accountName: params.analyticsAccountName ?? undefined,
        userName: params.analyticsUserName ?? undefined,
        httpMethod: req.method ?? undefined,
        bytesDeleted: options.bytesDeleted ?? undefined,
        bytesReceived: options.bytesReceived ?? undefined,
        bodyLength: options.bodyLength ?? undefined,
        contentLength: options.contentLength ?? undefined,
        // eslint-disable-next-line camelcase
        elapsed_ms: options.elapsed_ms ?? undefined,

        // AWS access server logs fields https://docs.aws.amazon.com/AmazonS3/latest/userguide/LogFormat.html
        startTime: timestampToDateTime643(params.startTimeUnixMS) ?? undefined, // AWS "Time" field
        requester: getRequester(authInfo) ?? undefined,
        operation: options.operation ?? undefined,
        requestURI: options.requestURI ?? undefined,
        errorCode: options.errorCode ?? undefined,
        objectSize: options.objectSize ?? undefined,
        totalTime: options.totalTime ?? undefined,
        turnAroundTime: options.turnAroundTime ?? undefined,
        referer: options.referer ?? undefined,
        userAgent: options.userAgent ?? undefined,
        versionId: options.versionId ?? undefined,
        signatureVersion: authInfo?.getAuthVersion() ?? undefined,
        cipherSuite: req.socket?.encrypted
            ? req.socket.getCipher()['standardName']
            : req.headers?.['x-ssl-cipher'] ?? undefined,
        authenticationType: authInfo?.getAuthType() ?? undefined,
        hostHeader: req.headers?.host ?? undefined,
        tlsVersion: req.socket?.encrypted
            ? req.socket.getCipher()['version']
            : req.headers?.['x-ssl-protocol'] ?? undefined,
        aclRequired: options.aclRequired ?? undefined,
        // hostID: undefined,         // NOT IMPLEMENTED
        // accessPointARN: undefined, // NOT IMPLEMENTED

        // Shared between AWS access server logs and Analytics logs
        bucketOwner: params.bucketOwner ?? undefined,
        bucketName: params.bucketName ?? undefined, // AWS "Bucket" field
        // eslint-disable-next-line camelcase
        req_id: options.requestID ?? undefined, // AWS "Request ID" field
        bytesSent: options.bytesSent ?? undefined,
        clientIP: getRemoteIPFromRequest(req) ?? undefined,  // AWS 'Remote IP' field
        httpCode: options.httpCode ?? undefined, // AWS "HTTP Status" field
        objectKey: options.objectKey ?? undefined, // AWS "Key" field

        // Scality server access logs extra fields
        logFormatVersion: SERVER_ACCESS_LOG_FORMAT_VERSION,
        // If backbeat is enabled, we set loggingEnabled to false
        // to prevent backbeat requests from getting to log courier.
        loggingEnabled: params.backbeat ? false : (params.enabled ?? undefined),
        loggingTargetBucket: params.loggingEnabled?.TargetBucket ?? undefined,
        loggingTargetPrefix: params.loggingEnabled?.TargetPrefix ?? undefined,
        awsAccessKeyID: authInfo?.getAccessKey() ?? undefined,
        raftSessionID: params.raftSessionID ?? undefined,

        // Rate Limiting fields (only present when rate limited)
        rateLimited: params.rateLimited ?? undefined,
        rateLimitSource: params.rateLimitSource ?? undefined,
    };
}

/**
 * Logs a BATCH.DELETE.OBJECT entry for individual object deletions in multi-object delete
 * @param {object} req - HTTP request object
 * @param {string} requestID - Request ID from the response
 * @param {string} objectKey - Key of the object being deleted
 * @param {number|null} objectSize - Size of the object, or null if object doesn't exist
 * @param {Error|null} error - Error object if deletion failed, null if successful
 * @return {undefined}
 */
function logBatchDeleteObject(req, requestID, objectKey, objectSize, error) {
    if (!req.serverAccessLog || !serverAccessLogger) {
        return;
    }

    const params = req.serverAccessLog;
    let httpCode = 204;
    let errorCode = undefined;

    if (error) {
        httpCode = error.code ? parseInt(error.code, 10) : 500;
        errorCode = error.message || 'InternalError';
    }

    const logEntry = buildLogEntry(req, params, {
        bytesDeleted: objectSize,
        contentLength: objectSize,
        operation: 'BATCH.DELETE.OBJECT',
        objectSize,
        httpCode,
        errorCode,
        objectKey,
        requestID,
        aclRequired: req.serverAccessLog?.aclRequired,
    });

    serverAccessLogger.write(`${JSON.stringify(logEntry)}\n`);
}

function logServerAccess(req, res) {
    if (!req.serverAccessLog || !res.serverAccessLog || !serverAccessLogger) {
        return;
    }

    const params = req.serverAccessLog;
    const errorCode = res.serverAccessLog.errorCode;
    const endTurnAroundTime = res.serverAccessLog.endTurnAroundTime;
    const requestID = res.serverAccessLog.requestID;
    const bytesSent = res.serverAccessLog.bytesSent;

    const logEntry = buildLogEntry(req, params, {
        bytesDeleted: params.analyticsBytesDeleted,
        bytesReceived: Number.isInteger(req.parsedContentLength) ? req.parsedContentLength : undefined,
        bodyLength: req.headers['content-length'] !== undefined
            ? parseInt(req.headers['content-length'], 10)
            : undefined,
        contentLength: getObjectSize(req, res),
        // eslint-disable-next-line camelcase
        elapsed_ms: calculateElapsedMS(params.startTime, params.onCloseEndTime),
        operation: getOperation(req),
        requestURI: getURI(req),
        errorCode,
        objectSize: params.objectSize ?? getObjectSize(req, res),
        totalTime: calculateTotalTime(params.startTime, params.onFinishEndTime),
        turnAroundTime: calculateTurnAroundTime(params.startTurnAroundTime, endTurnAroundTime),
        versionId: req.query?.versionId,
        aclRequired: req.serverAccessLog.aclRequired,
        referer: req.headers?.referer,
        userAgent: req.headers?.['user-agent'],
        requestID,
        bytesSent: getBytesSent(res, bytesSent),
        httpCode: res.statusCode,
        objectKey: params.objectKey,
    });


    if (params.internalLogRequestQueue && params.internalLogRequestQueue.length > 0) {
        if (logEntry.operation === 'REST.POST.MULTI_OBJECT_DELETE') {
            for (const entry of params.internalLogRequestQueue) {
                logBatchDeleteObject(req, requestID, entry.objectKey, entry.objectSize, entry.error);
            }
        } else if (logEntry.operation.includes('COPY')) {
            for (const entry of params.internalLogRequestQueue) {
                logCopySourceAccess(req, requestID, entry.operation, entry.sourceBucket,
                    entry.sourceObject, entry.objectSize);
            }
        }
    }

    serverAccessLogger.write(`${JSON.stringify(logEntry)}\n`);
}

/**
 * Logs a REST.COPY.OBJECT_GET or REST.COPY.PART_GET entry for the source side of copy operations
 * @param {object} req - HTTP request object
 * @param {string} requestID - Request ID from the response
 * @param {string} operation - Operation name ('REST.COPY.OBJECT_GET' or 'REST.COPY.PART_GET')
 * @param {string} sourceBucket - Source bucket name
 * @param {string} sourceObject - Source object key
 * @param {number|null} objectSize - Size of the source object, or null if unavailable
 * @return {undefined}
 */
function logCopySourceAccess(req, requestID, operation, sourceBucket, sourceObject, objectSize) {
    if (!req.sourceServerAccessLog || !serverAccessLogger) {
        return;
    }

    const params = req.sourceServerAccessLog;
    const error = params.error;
    let httpCode = 200;
    let errorCode = undefined;

    if (error) {
        httpCode = error.code ? parseInt(error.code, 10) : 500;
        errorCode = error.message || 'InternalError';
    }

    // Copy over necessary fields from main serverAccessLog
    if (req.serverAccessLog) {
        params.authInfo = req.serverAccessLog.authInfo;
        params.startTime = req.serverAccessLog.startTime;
        params.startTimeUnixMS = req.serverAccessLog.startTimeUnixMS;
        params.startTurnAroundTime = req.serverAccessLog.startTurnAroundTime;
        params.onFinishEndTime = req.serverAccessLog.onFinishEndTime;
        params.analyticsAccountName = req.serverAccessLog.analyticsAccountName;
        params.analyticsUserName = req.serverAccessLog.analyticsUserName;
        params.analyticsAction = req.serverAccessLog.analyticsAction;
    }

    const logEntry = buildLogEntry(req, params, {
        operation,
        objectKey: sourceObject,
        objectSize,
        httpCode,
        errorCode,
        requestID,
        aclRequired: params.aclRequired,
    });

    serverAccessLogger.write(`${JSON.stringify(logEntry)}\n`);
}

module.exports = {
    logServerAccess,
    setServerAccessLogger,
    ServerAccessLogger,
    initializeInternalLogRequestQueue,
    queueInternalLogRequest,

    // Exported for testing.
    getRemoteIPFromRequest,
    getOperation,
    getRequester,
    getURI,
    getObjectSize,
    getBytesSent,
    calculateTotalTime,
    calculateTurnAroundTime,
    timestampToDateTime643,
};
