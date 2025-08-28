const apiLogger = require('./apiLogger');
const { config } = require('../Config');

/**
 * Log a successful API operation if API logging is enabled
 *
 * @param {string} operation - The API operation name (e.g., 'putObject', 'createBucket')
 * @param {object} request - The HTTP request object
 * @param {object} log - The existing request logger (for request ID extraction)
 * @param {object} bucketMD - Optional bucket metadata object to extract owner info
 * @param {object} authInfo - Authentication info object to extract requester info
 * @param {number} httpStatus - Optional HTTP status code (defaults based on operation)
 * @param {number} bytesSent - Optional number of response bytes sent (defaults based on operation)
 * @param {number} objectSize - Optional object size in bytes (for object operations)
 * @param {object} streamingV4Params - Optional streaming V4 parameters (indicates SigV4)
 * @returns {undefined}
 */
function logAPIOperation(operation, request, log, bucketMD, authInfo, httpStatus, bytesSent, objectSize, streamingV4Params) {
    // Check if API logging is enabled
    if (!config.apiLog || !config.apiLog.enabled) {
        return;
    }

    // Determine resource type based on operation
    let resourceType = 'OBJECT';
    if (operation === 'createBucket' || operation === 'deleteBucket' || operation === 'getBucketLocation') {
        resourceType = 'BUCKET';
    }

    // Extract bucket owner from bucket metadata if available
    let bucketOwner = '-';
    if (bucketMD && typeof bucketMD.getOwner === 'function') {
        bucketOwner = bucketMD.getOwner();
    }

    // Extract remote IP from request headers and connection info
    let remoteIP = '-';
    if (request.headers) {
        // Check for forwarded IP headers (proxy/load balancer scenarios)
        remoteIP = request.headers['x-forwarded-for'] ||
                   request.headers['x-real-ip'] ||
                   request.headers['x-client-ip'] ||
                   request.headers['cf-connecting-ip']; // Cloudflare

        // x-forwarded-for can contain multiple IPs, take the first one
        if (remoteIP && remoteIP.includes(',')) {
            remoteIP = remoteIP.split(',')[0].trim();
        }
    }

    // Fallback to connection remote address if no forwarded headers
    if (!remoteIP || remoteIP === '-') {
        remoteIP = (request.connection && request.connection.remoteAddress) ||
                   (request.socket && request.socket.remoteAddress) ||
                   (request.ip) ||
                   '-';
    }

    // Extract requester information from authInfo
    let requester = '-';
    if (authInfo) {
        if (authInfo.isRequesterPublicUser && authInfo.isRequesterPublicUser()) {
            requester = '-'; // Unauthenticated requests
        } else if (authInfo.isRequesterAnIAMUser && authInfo.isRequesterAnIAMUser()) {
            // IAM user: include IAM user name and account
            const iamUserName = authInfo.getIAMdisplayName ? authInfo.getIAMdisplayName() : '';
            const accountName = authInfo.getAccountDisplayName ? authInfo.getAccountDisplayName() : '';
            requester = iamUserName && accountName ? `${iamUserName}:${accountName}` : authInfo.getCanonicalID();
        } else if (authInfo.getCanonicalID) {
            // Regular user: canonical user ID
            requester = authInfo.getCanonicalID();
        }
    }

    // Extract Request-URI (HTTP method + URL + query params + HTTP version)
    let requestURI = '-';
    if (request) {
        const method = request.method || 'UNKNOWN';
        const url = request.url || request.originalUrl || '/';
        const httpVersion = request.httpVersion || '1.1';
        requestURI = `"${method} ${url} HTTP/${httpVersion}"`;
    }

    // Determine HTTP status code based on operation if not provided
    let statusCode = httpStatus;
    if (!statusCode) {
        // Default success status codes for different operations
        switch (operation) {
            case 'createBucket':
                statusCode = 200; // Bucket creation
                break;
            case 'putObject':
                statusCode = 200; // Object upload
                break;
            case 'deleteObject':
                statusCode = 204; // Object deletion (No Content)
                break;
            default:
                statusCode = 200; // Generic success
        }
    }

    // Error code is always '-' for successful operations in this POC
    const errorCode = '-';

    // Determine bytes sent based on operation if not provided
    let responseBytesSent = bytesSent;
    if (responseBytesSent === undefined || responseBytesSent === null) {
        // Default response sizes for different operations (excluding HTTP overhead)
        switch (operation) {
            case 'createBucket':
                responseBytesSent = 0; // Bucket creation typically returns empty body
                break;
            case 'putObject':
                responseBytesSent = 0; // Object upload typically returns empty body
                break;
            case 'deleteObject':
                responseBytesSent = 0; // Object deletion returns no content (204)
                break;
            default:
                responseBytesSent = 0; // Most S3 operations return minimal response
        }
    }

    // Convert to '-' if zero bytes as per AWS specification
    const bytesSentFormatted = responseBytesSent === 0 ? '-' : responseBytesSent;

    // Determine object size based on operation and available data
    let objSize = objectSize;
    if (objSize === undefined || objSize === null) {
        // For bucket operations, object size is not applicable
        if (operation === 'createBucket' || operation === 'deleteBucket' || operation === 'getBucketLocation') {
            objSize = '-';
        } else {
            // For object operations, try to extract from request or use '-' if not available
            objSize = request.parsedContentLength ||
                      (request.headers && request.headers['content-length']) ||
                      '-';
        }
    }

    // Format object size according to AWS specification
    const objectSizeFormatted = (objSize === '-' || objSize === 0) ? '-' : objSize;

    // Determine signature version based on available information
    let signatureVersion = '-';
    if (authInfo && !authInfo.isRequesterPublicUser()) {
        // Check for streaming V4 parameters (indicates SigV4)
        if (streamingV4Params && typeof streamingV4Params === 'object') {
            signatureVersion = 'SigV4';
        } else if (request.headers) {
            // Check authorization header for signature version indicators
            const authHeader = request.headers.authorization || request.headers.Authorization || '';
            if (authHeader.includes('AWS4-HMAC-SHA256')) {
                signatureVersion = 'SigV4';
            } else if (authHeader.startsWith('AWS ')) {
                signatureVersion = 'SigV2';
            } else if (request.query && (request.query['X-Amz-Algorithm'] || request.query['Signature'])) {
                // Pre-signed URL with SigV4 or SigV2
                if (request.query['X-Amz-Algorithm'] === 'AWS4-HMAC-SHA256') {
                    signatureVersion = 'SigV4';
                } else if (request.query['Signature']) {
                    signatureVersion = 'SigV2';
                }
            }
        }
    }

    // Determine if logging is enabled for this bucket
    const bucketName = request.bucketName || '-';
    const loggingEnabled = bucketName !== '-' &&
                          config.apiLog &&
                          config.apiLog.enabledBuckets &&
                          config.apiLog.enabledBuckets.includes(bucketName);

    const logData = {
        BucketOwner: bucketOwner,
        Bucket: bucketName,
        RemoteIP: remoteIP,
        Requester: requester,
        RequestURI: requestURI,
        Operation: `REST.${request.method}.${resourceType}`,
        Key: request.objectKey || '-',
        RequestID: (log && typeof log.getSerializedUids === 'function') ? log.getSerializedUids() : '-',
        HTTPStatus: statusCode,
        ErrorCode: errorCode,
        BytesSent: bytesSentFormatted,
        ObjectSize: objectSizeFormatted,
        SignatureVersion: signatureVersion,
        logging_enabled: loggingEnabled
    };

    // Log the API operation as JSON
    apiLogger.info('API_OPERATION', logData);
}

module.exports = {
    logAPIOperation
};
