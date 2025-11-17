const { Werelogs } = require('werelogs');
const { config } = require('../Config');
const fs = require('fs');
const path = require('path');
const logger = require('./logger');

const DEFAULT_OUTPUT_FILE = '/logs/api-operations.log';
const SERVER_ACCESS_LOG_FORMAT_VERSION = '0';

function createServerAccessLogger() {
    if (!config.serverAccessLogs || !config.serverAccessLogs.enabled) {
        logger.warn('ServerAccessLogs disabled returning no-op logger');
        return {
            info: () => { },
            debug: () => { },
            warn: () => { },
            error: () => { },
            trace: () => { },
            fatal: () => { },
        };
    }

    // Ensure logs directory exists
    const outputFile = config.serverAccessLogs.outputFile || DEFAULT_OUTPUT_FILE;
    const logDir = path.dirname(outputFile);

    try {
        if (!fs.existsSync(logDir)) {
            fs.mkdirSync(logDir, { recursive: true });
        }
    } catch (error) {
        // Fall back to logger-only logging if directory creation fails
        logger.warn('Failed to create ServerAccess log directory, falling back to console logging:', error.message);

        const apiWerelogs = new Werelogs({
            level: config.serverAccessLogs.logLevel || 'info',
            dump: config.serverAccessLogs.dumpLevel || 'error',
            streams: [
                { level: 'trace', stream: process.stdout }
            ]
        });

        return new apiWerelogs.Logger('ServerAccessLogger');
    }

    // Create file stream for API logs
    const serverAccessLogStream = fs.createWriteStream(outputFile, { flags: 'a' });

    // Handle stream errors
    serverAccessLogStream.on('error', error => {
        logger.error('ServerAccessLogger log file stream error:', error);
    });

    // Create the API-specific Werelogs instance - file output only
    const apiWerelogs = new Werelogs({
        level: config.serverAccessLogs.logLevel || 'info',
        dump: config.serverAccessLogs.dumpLevel || 'error',
        streams: [{ level: 'trace', stream: serverAccessLogStream }]
    });
    logger.info('ServerAccessLogger created successfully');
    return new apiWerelogs.Logger('ServerAccessLogger');
}

var serverAccessLogger = {
    info: () => { },
    debug: () => { },
    warn: () => { },
    error: () => { },
    trace: () => { },
    fatal: () => { },
};


try {
    serverAccessLogger = createServerAccessLogger();
} catch (error) {
    logger.error('Failed to create ServiceAccessLogger, using no-op logger:', error);
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
        const url = request.url || request.originalUrl || '/';
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
        return len || null;
    }

    if (request && objectSizePutMethods[request.apiMethod]) {
        const len = request.headers['content-length'];
        return len || null;
    }

    return null;
}

function getBytesSent(res, bytesSent) {
    if (bytesSent) {
        return bytesSent;
    }

    if (!res) {
        return null;
    }

    const len = res.getHeader('Content-Length');
    return len || null;
}

function calculateTotalTime(startTime, endTime) {
    if (!startTime || !endTime) {
        return null;
    }

    return ((endTime - startTime) / 1_000_000n).toString();
}

function calculateTurnAroundTime(startTurnAroundTime, endTurnAroundTime) {
    if (!startTurnAroundTime || !endTurnAroundTime) {
        return null;
    }

    return ((endTurnAroundTime - startTurnAroundTime) / 1_000_000n).toString();
}

function logServerAccess(req, res) {
    const params = req.serverAccessLog;
    const errorCode = res.serverAccessLog.errorCode;
    const endTurnAroundTime = res.serverAccessLog.endTurnAroundTime;
    const requestID = res.serverAccessLog.requestID;
    const bytesSent = res.serverAccessLog.bytesSent;
    const authInfo = params.authInfo;

    serverAccessLogger.info('', {
        // Analytics
        action: params.analyticsAction || null,
        accountName: params.analyticsAccountName || null,
        accountDisplayName: authInfo ? authInfo.getAccountDisplayName() : null,
        userName: params.analyticsUserName || null,
        clientPort: req.socket.remotePort || null,
        httpMethod: req.method || null,
        bytesDeleted: params.analyticsBytesDeleted || null,
        bytesReceived: req.parsedContentLength || 0,
        bodyLength: parseInt(req.headers['content-length'], 10) || 0,
        contentLength: req.parsedContentLength || 0,
        // eslint-disable-next-line camelcase
        elapsed_ms: params.startTime && params.onCloseEndTime ?
            Number(params.onCloseEndTime - params.startTime) / 1_000_000 : null,
        httpURL: req.url || null,

        // AWS access server logs fields https://docs.aws.amazon.com/AmazonS3/latest/userguide/LogFormat.html
        startTime: params.startTimeUnixMS || null, // AWS "Time" field
        requester: getRequester(authInfo),
        operation: getOperation(req),
        requestURI: getURI(req),
        errorCode: errorCode || null,
        objectSize: getObjectSize(req, res),
        totalTime: calculateTotalTime(params.startTime, params.endTime),
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
    });
}

module.exports = {
    logServerAccess,
};
