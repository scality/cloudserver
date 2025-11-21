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

function getOperation(req) {
    const methodToResType = Object.freeze({
        'bucketDelete': 'BUCKET',
        'bucketDeleteCors': 'BUCKET',
        'bucketDeleteEncryption': 'BUCKET',
        'bucketDeleteWebsite': 'BUCKET',
        'bucketGet': 'BUCKET',
        'bucketGetACL': 'BUCKET',
        'bucketGetCors': 'BUCKET',
        'bucketGetObjectLock': 'BUCKET',
        'bucketGetVersioning': 'VERSIONING',
        'bucketGetWebsite': 'BUCKET',
        'bucketGetLocation': 'BUCKET',
        'bucketGetEncryption': 'BUCKET',
        'bucketHead': 'BUCKET',
        'bucketPut': 'BUCKET',
        'bucketPutACL': 'BUCKET',
        'bucketPutCors': 'BUCKET',
        'bucketPutVersioning': 'VERSIONING',
        'bucketPutTagging': 'BUCKET',
        'bucketDeleteTagging': 'BUCKET',
        'bucketGetTagging': 'BUCKET',
        'bucketPutWebsite': 'BUCKET',
        'bucketPutReplication': 'BUCKET',
        'bucketGetReplication': 'BUCKET',
        'bucketDeleteReplication': 'BUCKET',
        'bucketDeleteQuota': 'BUCKET',
        'bucketPutLifecycle': 'BUCKET',
        'bucketUpdateQuota': 'BUCKET',
        'bucketGetLifecycle': 'BUCKET',
        'bucketDeleteLifecycle': 'BUCKET',
        'bucketPutPolicy': 'BUCKETPOLICY',
        'bucketGetPolicy': 'BUCKETPOLICY',
        'bucketGetQuota': 'BUCKET',
        'bucketDeletePolicy': 'BUCKETPOLICY',
        'bucketPutObjectLock': 'BUCKET',
        'bucketPutNotification': 'BUCKET',
        'bucketGetNotification': 'BUCKET',
        'bucketPutEncryption': 'BUCKET',
        'bucketPutLogging': 'LOGGING_STATUS',
        'bucketGetLogging': 'LOGGING_STATUS',
        // 'corsPreflight': '',
        'completeMultipartUpload': 'OBJECT',
        'initiateMultipartUpload': 'OBJECT',
        'listMultipartUploads': 'OBJECT',
        'listParts': 'OBJECT',
        'metadataSearch': 'OBJECT',
        'multiObjectDelete': 'OBJECT',
        'multipartDelete': 'OBJECT',
        'objectDelete': 'OBJECT',
        'objectDeleteTagging': 'OBJECT',
        'objectGet': 'OBJECT',
        'objectGetACL': 'OBJECT',
        'objectGetLegalHold': 'OBJECT',
        'objectGetRetention': 'OBJECT',
        'objectGetTagging': 'OBJECT',
        'objectCopy': 'OBJECT',
        'objectHead': 'OBJECT',
        'objectPut': 'OBJECT',
        'objectPutACL': 'OBJECT',
        'objectPutLegalHold': 'OBJECT',
        'objectPutTagging': 'OBJECT',
        'objectPutPart': 'OBJECT',
        'objectPutCopyPart': 'OBJECT',
        'objectPutRetention': 'OBJECT',
        'objectRestore': 'OBJECT',
        // 'serviceGet': '',
        // 'websiteGet': '',
        // 'websiteHead': '',
    });

    return `REST.${req.method}.${methodToResType[req.apiMethod] ? methodToResType[req.apiMethod] : 'UNKNOWN'}`;
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

function getObjectSize(request, response) {
    const objectSizePutMethods = Object.freeze({
        'objectPut': true,
        'objectPutPart': true,
    });

    const objectSizeGetMethods = Object.freeze({
        'objectGet': true,
    });

    // If it is a PUT get the Content-Length from the request, if it is a GET get it from the response.
    if (request && response && objectSizeGetMethods[request.apiMethod]) {
        const len = response.getHeader('Content-Length');
        if (len === undefined || len === null) {
            return null;
        }

        return len;
    }

    if (request && objectSizePutMethods[request.apiMethod]) {
        const len = request.headers['content-length'];
        if (len === undefined || len === null) {
            return null;
        }

        return len;
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

function logServerAccess(req, res) {
    if (!req.serverAccessLog || !res.serverAccessLog || !serverAccessLogger) {
        return;
    }

    const params = req.serverAccessLog;
    const errorCode = res.serverAccessLog.errorCode;
    const endTurnAroundTime = res.serverAccessLog.endTurnAroundTime;
    const requestID = res.serverAccessLog.requestID;
    const bytesSent = res.serverAccessLog.bytesSent;
    const authInfo = params.authInfo;

    serverAccessLogger.write(`${JSON.stringify({
        // Werelog fields
        // logs.access_logs_ingest.timestamp in Clickhouse is of type DateTime so it expects seconds as an integer.
        time: Math.floor(Date.now() / 1000),
        hostname,
        pid: process.pid,

        // Analytics
        action: params.analyticsAction || null,
        accountName: params.analyticsAccountName || null,
        accountDisplayName: authInfo ? authInfo.getAccountDisplayName() : null,
        userName: params.analyticsUserName || null,
        clientPort: req.socket.remotePort || null,
        httpMethod: req.method || null,
        bytesDeleted: params.analyticsBytesDeleted || params.analyticsBytesDeleted === 0 ? 0 : null,
        bytesReceived: req.parsedContentLength || 0,
        bodyLength: parseInt(req.headers['content-length'], 10) || 0,
        contentLength: req.parsedContentLength || 0,
        // eslint-disable-next-line camelcase
        elapsed_ms: calculateElapsedMS(params.startTime, params.onCloseEndTime),
        httpURL: req.url || null,

        // AWS access server logs fields https://docs.aws.amazon.com/AmazonS3/latest/userguide/LogFormat.html
        startTime: timestampToDateTime643(params.startTimeUnixMS), // AWS "Time" field
        requester: getRequester(authInfo),
        operation: getOperation(req),
        requestURI: getURI(req),
        errorCode: errorCode || null,
        objectSize: getObjectSize(req, res),
        totalTime: calculateTotalTime(params.startTime, params.onFinishEndTime),
        turnAroundTime: calculateTurnAroundTime(params.startTurnAroundTime, endTurnAroundTime),
        referer: req.headers.referer || null,
        userAgent: req.headers['user-agent'] || null,
        versionID: req.query ? req.query.versionId || null : null,
        signatureVersion: authInfo ? authInfo.getAuthVersion() : null,
        cipherSuite: req.socket.encrypted ? req.socket.getCipher()['standardName'] : null,
        authenticationType: authInfo ? authInfo.getAuthType() : null,
        hostHeader: req.headers.host || null,
        tlsVersion: req.socket.encrypted ? req.socket.getCipher()['version'] : null,
        aclRequired: null,       // TODO: CLDSRV-774
        // hostID: null,         // NOT IMPLEMENTED
        // accessPointARN: null, // NOT IMPLEMENTED

        // Shared between AWS access server logs and Analytics logs
        bucketOwner: params.bucketOwner || null,
        bucketName: params.bucketName || null,  // AWS "Bucket" field
        // eslint-disable-next-line camelcase
        req_id: requestID || null,              // AWS "Request ID" field
        bytesSent: getBytesSent(res, bytesSent),
        clientIP: getRemoteIPFromRequest(req),  // AWS 'Remote IP' field
        httpCode: res.statusCode || null,       // AWS "HTTP Status" field
        objectKey: params.objectKey || null,    // AWS "Key" field

        // Scality server access logs extra fields
        logFormatVersion: SERVER_ACCESS_LOG_FORMAT_VERSION,
        loggingEnabled: params.enabled,
        loggingTargetBucket: params.loggingEnabled ? params.loggingEnabled.TargetBucket : null,
        loggingTargetPrefix: params.loggingEnabled ? params.loggingEnabled.TargetPrefix : null,
        awsAccessKeyID: authInfo ? authInfo.getAccessKey() : null,
        raftSessionID: params.raftSessionID || null,
    })}\n`);
}

module.exports = {
    logServerAccess,
    setServerAccessLogger,
    ServerAccessLogger,

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
