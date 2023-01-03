const { errors, jsutil } = require('arsenal');
const { Readable } = require('stream');
const collectResponseHeaders = require('../../utilities/collectResponseHeaders');
const collectCorsHeaders = require('../../utilities/collectCorsHeaders');
const crypto = require('crypto');
const { prepareStream } = require('arsenal/build/lib/s3middleware/prepareStream');

/**
 * Decodes an URI and return the result.
 * Do the same decoding than in S3 server
 * @param {string} uri - uri to decode
 * @returns {string} -
 */
function _decodeURI(uri) {
    return decodeURIComponent(uri.replace(/\+/g, ' '));
}

/**
 * Generic function to get data from a client request.
 *
 * @param {object} request - incoming request
 * @param {object} log - logger object
 * @param {function} callback -
 * @returns {undefined}
 */
function receiveData(request, log, callback) {
    // Get keycontent
    const { parsedContentLength } = request;
    const ContentLengthThreshold = 1024 * 1024; // 1MB
    // Prevent memory overloads by limiting the size of the
    // received data.
    if (parsedContentLength > ContentLengthThreshold) {
        return callback(errors.InvalidInput
            .customizeDescription(`maximum allowed content-length is ${ContentLengthThreshold} bytes`));
    }
    const value = Buffer.alloc(parsedContentLength);
    const cbOnce = jsutil.once(callback);
    const dataStream = prepareStream(request, request.streamingV4Params, log, cbOnce);
    let cursor = 0;
    let exceeded = false;
    dataStream.on('data', data => {
        if (cursor + data.length > parsedContentLength) {
            exceeded = true;
        }
        if (!exceeded) {
            data.copy(value, cursor);
        }
        cursor += data.length;
    });
    dataStream.on('end', () => {
        if (exceeded) {
            log.error('data stream exceed announced size',
                { parsedContentLength, overflow: cursor });
            return callback(errors.InternalError);
        } else {
            return callback(null, value.toString());
        }
    });
    return undefined;
}

/**
 * Builds a valid XML file for SOSAPI
 *
 * @param {string} xmlContent - valid xml content
 * @returns {string} a valid and formatted XML file
 */
function buildHeadXML(xmlContent) {
    return `<?xml version="1.0" encoding="UTF-8" ?>\n${xmlContent}\n`;
}

/**
 * Get response headers for the object
 * @param {object} request - incoming request
 * @param {string} bucket - bucket name
 * @param {string} dataBuffer - data to send as a buffer
 * @param {date} [lastModified] - last modified date of the value
 * @param {object} log - logging object
 * @returns {object} - response headers
 */
function getResponseHeader(request, bucket, dataBuffer, lastModified, log) {
    const corsHeaders = collectCorsHeaders(request.headers.origin,
        request.method, bucket);
    const responseMetaHeaders = collectResponseHeaders({
        'last-modified': lastModified || new Date().toISOString(),
        'content-md5': crypto
            .createHash('md5')
            .update(dataBuffer)
            .digest('hex'),
        'content-length': dataBuffer.byteLength,
        'content-type': 'text/xml',
    }, corsHeaders, null, false);
    responseMetaHeaders.versionId = 'null';
    responseMetaHeaders['x-amz-id-2'] = log.getSerializedUids();
    responseMetaHeaders['x-amz-request-id'] = log.getSerializedUids();
    return responseMetaHeaders;
}
/**
 * Generic function to respond to user with data using streams
 *
 * @param {object} request - incoming request
 * @param {object} response - response object
 * @param {object} log - logging object
 * @param {string} bucket - bucket name
 * @param {string} data - data to send
 * @param {date} [lastModified] - last modified date of the value
 * @returns {undefined} -
 */
function respondWithData(request, response, log, bucket, data, lastModified) {
    const dataBuffer = Buffer.from(data);
    const responseMetaHeaders = getResponseHeader(request, bucket, dataBuffer, lastModified, log);

    response.on('finish', () => {
        let contentLength = 0;
        if (responseMetaHeaders && responseMetaHeaders['Content-Length']) {
            contentLength = responseMetaHeaders['Content-Length'];
        }
        log.end().addDefaultFields({ contentLength });
        log.end().info('responded with streamed content', {
            httpCode: response.statusCode,
        });
    });

    if (responseMetaHeaders && typeof responseMetaHeaders === 'object') {
        Object.keys(responseMetaHeaders).forEach(key => {
            if (responseMetaHeaders[key] !== undefined) {
                try {
                    response.setHeader(key, responseMetaHeaders[key]);
                } catch (e) {
                    log.debug('header can not be added ' +
                        'to the response', {
                            header: responseMetaHeaders[key],
                        error: e.stack, method: 'routeVeeam/respondWithData'
                    });
                }
            }
        });
    }

    response.writeHead(200);
    const stream = Readable.from(dataBuffer);
    stream.pipe(response);
    stream.on('unpipe', () => {
        response.end();
    });
    stream.on('error', () => {
        response.end();
    });
}

const validPath = '.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/';

module.exports = {
    _decodeURI,
    receiveData,
    respondWithData,
    getResponseHeader,
    buildHeadXML,
    validPath,
};
