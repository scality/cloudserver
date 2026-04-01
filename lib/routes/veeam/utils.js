const xml2js = require('xml2js');
const { errors, errorInstances, jsutil } = require('arsenal');
const { Readable } = require('stream');
const collectResponseHeaders = require('../../utilities/collectResponseHeaders');
const collectCorsHeaders = require('../../utilities/collectCorsHeaders');
const crypto = require('crypto');
const { prepareStream } = require('arsenal/build/lib/s3middleware/prepareStream');
const UtilizationService = require('../../utilization/instance');
const metadata = require('../../metadata/wrapper');

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
        return callback(errorInstances.InvalidInput
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
 * @param {BucketInfo} bucket - bucket
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
 * @param {BucketInfo} bucket - bucket info
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

/**
 * Helper to determine if the current requested file is system.xml
 *
 * @param {string} objectKey - object key
 * @returns {boolean} - true if the object key ends with `/system.xml`
 */
function isSystemXML(objectKey) {
    return objectKey.endsWith('/system.xml');
}

/**
 * Helper to extract the file from the bucket metadata
 *
 * @param {object} request - incoming request
 * @param {object} data - the bucket metadata or input data
 * @param {boolean} inlineLastModified - true if LastModified should be in the returned object
 * or as another standalone field
 * @returns {error | object} - error if file does not exist, or
 * the associated metadata
 */
function getFileToBuild(request, data, inlineLastModified = false) {
    const _isSystemXML = isSystemXML(request.objectKey);
    const fileToBuild = _isSystemXML ? data?.SystemInfo : data?.CapacityInfo;

    if (!fileToBuild) {
        return { error: errors.NoSuchKey };
    }

    const modified = fileToBuild.LastModified || (new Date()).toISOString();
    const fieldName = _isSystemXML ? 'SystemInfo' : 'CapacityInfo';

    if (inlineLastModified) {
        fileToBuild.LastModified = modified;
        return {
            value: {
                [fieldName]: fileToBuild,
            },
            fieldName,
        };
    } else {
        delete fileToBuild.LastModified;
        return {
            value: {
                [fieldName]: fileToBuild,
            },
            fieldName,
        };
    }
}

/**
 * Fetches capacity metrics from UtilizationService for a bucket.
 * Handles 404 gracefully (no metrics available yet, e.g. post-install),
 * returning a default bucketMetrics with the current date so callers always
 * receive a usable object.
 *
 * @param {object} bucketMd - bucket metadata
 * @param {object} request - incoming request
 * @param {object} log - logger object
 * @param {string} method - calling method name for log context
 * @param {function} callback - (err, bucketMetrics) where bucketMetrics always
 *   has at least a `date` field; on a real 404 the date defaults to new Date()
 * @returns {undefined}
 */
function fetchCapacityMetrics(bucketMd, request, log, method, callback) {
    const bucketKey = `${bucketMd._name}_${new Date(bucketMd._creationDate).getTime()}`;
    return UtilizationService.getUtilizationMetrics('bucket', bucketKey, null, {}, (err, bucketMetrics) => {
        if (err) {
            const statusCode = err.response?.status || err.statusCode || err.code;
            if (statusCode === 404) {
                log.warn('UtilizationService returned 404 when fetching capacity metrics', {
                    method,
                    bucket: request.bucketName,
                    error: err.message || err.code,
                });
                return callback(null, { date: new Date() });
            }
            log.error('error fetching capacity metrics from UtilizationService', {
                method,
                bucket: request.bucketName,
                error: err.message || err.code,
                statusCode,
            });
            return callback(err);
        }
        return callback(null, bucketMetrics);
    });
}

/**
 * Builds Veeam file data (XML content + response metadata) for a given request.
 *
 * @param {object} request - incoming request
 * @param {object} bucketMd - bucket metadata from the router
 * @param {object} log - logger object
 * @param {string} name - calling method name (for log context)
 * @param {function} callback - (err, result) where result is { xmlContent, dataBuffer, modified, bucketData }
 * @returns {undefined}
 */
function buildVeeamFileData(request, bucketMd, log, name, callback) {
    return metadata.getBucket(request.bucketName, log, (err, data) => {
        if (err) {
            return callback(errors.InternalError);
        }

        const fileToBuild = getFileToBuild(request, data._capabilities?.VeeamSOSApi);

        if (fileToBuild.error) {
            return callback(fileToBuild.error);
        }

        const finalize = bucketMetrics => {
            const modified = bucketMetrics.date;
            if (
                bucketMetrics.bytesTotal !== undefined
                    && fileToBuild.value.CapacityInfo
                    && !fileToBuild.value.CapacityInfo.Used
            ) {
                fileToBuild.value.CapacityInfo.Used = Number(bucketMetrics.bytesTotal);
                fileToBuild.value.CapacityInfo.Available =
                    Number(fileToBuild.value.CapacityInfo.Capacity) - Number(bucketMetrics.bytesTotal);
                // TODO CLDSRV-633 when SUR backend supports realtime metrics: it will
                // report the real last cseq/date processed by SUR, instead of the current date,
                // ensuring no issue in a SOSAPI context. We should use this information.
            }

            const builder = new xml2js.Builder({ headless: true });
            const xmlContent = buildHeadXML(builder.buildObject(fileToBuild.value));
            const dataBuffer = Buffer.from(xmlContent);
            return callback(null, { xmlContent, dataBuffer, modified, bucketData: data });
        };

        if (!isSystemXML(request.objectKey)) {
            return fetchCapacityMetrics(bucketMd, request, log, name, (fetchErr, bucketMetrics) => {
                if (fetchErr) {
                    return callback(errors.InternalError);
                }
                return finalize(bucketMetrics);
            });
        }
        return finalize({ date: new Date() });
    });
}

module.exports = {
    _decodeURI,
    receiveData,
    respondWithData,
    getResponseHeader,
    buildHeadXML,
    validPath,
    isSystemXML,
    getFileToBuild,
    fetchCapacityMetrics,
    buildVeeamFileData,
};
