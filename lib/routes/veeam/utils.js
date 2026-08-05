const xml2js = require('xml2js');
const { errors, errorInstances } = require('arsenal');
const { Readable, Writable, pipeline: streamPipeline } = require('stream');
const { promisify } = require('util');
const collectResponseHeaders = require('../../utilities/collectResponseHeaders');
const collectCorsHeaders = require('../../utilities/collectCorsHeaders');
const crypto = require('crypto');
const { prepareStream } = require('../../api/apiUtils/object/prepareStream');
const {
    getChecksumDataFromHeaders,
    arsenalErrorFromChecksumError,
    defaultChecksumData,
    areChecksumsEnabled,
} = require('../../api/apiUtils/integrity/validateChecksums');
const UtilizationService = require('../../utilization/instance');
const metadata = require('../../metadata/wrapper');

const pipeline = promisify(streamPipeline);
const getBucket = promisify((...args) => metadata.getBucket(...args));

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
 * The request stream is decoded and validated according to its
 * x-amz-content-sha256 value (plain, signed streaming or unsigned streaming
 * with trailing checksum), with the same semantics as the object data path.
 *
 * @param {object} request - incoming request
 * @param {object} log - logger object
 * @returns {Promise<string>}
 */
async function receiveData(request, log) {
    const { parsedContentLength } = request;
    const ContentLengthThreshold = 1024 * 1024; // 1MB

    // Prevent memory overloads by limiting the size of the
    // received data.
    if (parsedContentLength > ContentLengthThreshold) {
        throw errorInstances.InvalidInput.customizeDescription(
            `maximum allowed content-length is ${ContentLengthThreshold} bytes`,
        );
    }
    const headerChecksum = areChecksumsEnabled() ? getChecksumDataFromHeaders(request.headers) : null;
    if (headerChecksum && headerChecksum.error) {
        throw arsenalErrorFromChecksumError(headerChecksum);
    }
    const checksums = { primary: headerChecksum || defaultChecksumData, secondary: null };
    let totalLength = 0;
    const chunks = [];
    const collector = new Writable({
        write(chunk, _enc, cb) {
            totalLength += chunk.length;
            if (totalLength > parsedContentLength) {
                log.error('data stream exceed announced size', { parsedContentLength, overflow: totalLength });
                return cb(
                    errorInstances.InvalidRequest.customizeDescription(
                        'request body exceeds the announced content-length',
                    ),
                );
            }
            chunks.push(chunk);
            return cb();
        },
    });
    // Transform errors can be delivered through the errCb side-channel
    // without necessarily erroring the pipeline, so bridge them into a
    // promise raced against the pipeline completion. A repeated rejection
    // after settlement is a no-op; the no-op handler below keeps the
    // rejection handled even on paths that throw before the race subscribes.
    let onStreamError;
    const streamError = new Promise((resolve, reject) => {
        onStreamError = reject;
    });
    streamError.catch(() => {});
    const prepared = prepareStream(request, request.streamingV4Params, checksums, log, onStreamError);
    if (prepared.error) {
        throw prepared.error;
    }
    await Promise.race([pipeline(prepared.stream, collector), streamError]);
    // Checksum transforms only compute digests while streaming: validation
    // against the expected values (header or trailer) must be done once the
    // stream is fully consumed.
    // Validate the primary stream explicitly rather than relying on
    // `prepared.stream` happening to be that transform. It is absent when
    // checksums are disabled, in which case there is nothing to validate.
    const checksumErr =
        (prepared.contentSHA256Stream && prepared.contentSHA256Stream.validateChecksum()) ||
        (prepared.primaryChecksumStream && prepared.primaryChecksumStream.validateChecksum());
    if (checksumErr) {
        log.debug('failed checksum validation', { error: checksumErr });
        throw arsenalErrorFromChecksumError(checksumErr);
    }
    return Buffer.concat(chunks).toString();
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
    const corsHeaders = collectCorsHeaders(request.headers.origin, request.method, bucket);
    const responseMetaHeaders = collectResponseHeaders(
        {
            'last-modified': lastModified || new Date().toISOString(),
            'content-md5': crypto.createHash('md5').update(dataBuffer).digest('hex'),
            'content-length': dataBuffer.byteLength,
            'content-type': 'text/xml',
        },
        corsHeaders,
        null,
        false,
    );
    responseMetaHeaders.versionId = 'null';
    responseMetaHeaders['x-amz-id-2'] = log.getSerializedUids();
    responseMetaHeaders['x-amz-request-id'] = log.getSerializedUids();
    return responseMetaHeaders;
}
/**
 * Builds a headless XML string wrapped in the standard SOSAPI XML declaration.
 *
 * @param {object} obj - JS object to serialize to XML
 * @returns {string} formatted XML file content
 */
function buildXML(obj) {
    const builder = new xml2js.Builder({ headless: true });
    return buildHeadXML(builder.buildObject(obj));
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
 * @returns {Promise<void>} -
 */
async function respondWithData(request, response, log, bucket, data, lastModified) {
    const dataBuffer = Buffer.from(data);
    const responseMetaHeaders = getResponseHeader(request, bucket, dataBuffer, lastModified, log);

    if (responseMetaHeaders && typeof responseMetaHeaders === 'object') {
        Object.keys(responseMetaHeaders).forEach(key => {
            if (responseMetaHeaders[key] !== undefined) {
                try {
                    response.setHeader(key, responseMetaHeaders[key]);
                } catch (e) {
                    log.debug('header can not be added ' + 'to the response', {
                        header: responseMetaHeaders[key],
                        error: e.stack,
                        method: 'routeVeeam/respondWithData',
                    });
                }
            }
        });
    }

    response.writeHead(200);

    let contentLength = 0;
    if (responseMetaHeaders && responseMetaHeaders['Content-Length']) {
        contentLength = responseMetaHeaders['Content-Length'];
    }
    log.end().addDefaultFields({ contentLength });

    try {
        // Use a single-element array so the Buffer is sent as one chunk rather
        // than being iterated byte-by-byte by Readable.from.
        await pipeline(Readable.from([dataBuffer]), response);
        log.end().info('responded with streamed content', {
            httpCode: response.statusCode,
        });
    } catch (err) {
        log.end().error('error streaming response', {
            httpCode: response.statusCode,
            error: err.message,
        });
    }
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

    const modified = fileToBuild.LastModified || new Date().toISOString();
    const fieldName = _isSystemXML ? 'SystemInfo' : 'CapacityInfo';

    if (inlineLastModified) {
        fileToBuild.LastModified = modified;
    } else {
        delete fileToBuild.LastModified;
    }

    return {
        value: {
            [fieldName]: fileToBuild,
        },
        fieldName,
    };
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
 * @returns {Promise<object>} bucketMetrics always has at least a `date` field;
 *   on a real 404 the date defaults to new Date()
 */
async function fetchCapacityMetrics(bucketMd, request, log) {
    const bucketKey = `${bucketMd._name}_${new Date(bucketMd._creationDate).getTime()}`;
    try {
        return await UtilizationService.getUtilizationMetrics('bucket', bucketKey, null, {}, log);
    } catch (err) {
        const statusCode = err.response?.status || err.statusCode || err.code;
        if (statusCode === 404) {
            log.warn('UtilizationService returned 404 when fetching capacity metrics', {
                bucket: request.bucketName,
                error: err.message || err.code,
            });
            return { date: new Date() };
        }
        log.error('error fetching capacity metrics from UtilizationService', {
            bucket: request.bucketName,
            error: err.message || err.code,
            statusCode,
        });
        throw err;
    }
}

/**
 * Builds Veeam file data (XML content + response metadata) for a given request.
 *
 * @param {object} request - incoming request
 * @param {object} bucketMd - bucket metadata from the router
 * @param {object} log - logger object
 * @returns {Promise<object>} result with { xmlContent, dataBuffer, modified, bucketData }
 */
async function buildVeeamFileData(request, bucketMd, log) {
    let data;
    try {
        data = await getBucket(request.bucketName, log);
    } catch {
        log.error('error fetching bucket metadata', { bucket: request.bucketName });
        throw errors.InternalError;
    }

    const fileToBuild = getFileToBuild(request, data._capabilities?.VeeamSOSApi);

    if (fileToBuild.error) {
        throw fileToBuild.error;
    }

    let bucketMetrics;
    if (!isSystemXML(request.objectKey)) {
        try {
            bucketMetrics = await fetchCapacityMetrics(bucketMd, request, log);
        } catch {
            log.error('error fetching capacity metrics for bucket', { bucket: request.bucketName });
            throw errors.InternalError;
        }
    } else {
        bucketMetrics = { date: new Date() };
    }

    const modified = bucketMetrics.date;
    if (
        bucketMetrics.bytesTotal !== undefined &&
        fileToBuild.value.CapacityInfo &&
        !fileToBuild.value.CapacityInfo.Used
    ) {
        fileToBuild.value.CapacityInfo.Used = Number(bucketMetrics.bytesTotal);
        fileToBuild.value.CapacityInfo.Available =
            Number(fileToBuild.value.CapacityInfo.Capacity) - Number(bucketMetrics.bytesTotal);
        // TODO CLDSRV-633 when SUR backend supports realtime metrics: it will
        // report the real last cseq/date processed by SUR, instead of the current date,
        // ensuring no issue in a SOSAPI context. We should use this information.
    }

    const xmlContent = buildXML(fileToBuild.value);
    const dataBuffer = Buffer.from(xmlContent);
    return { xmlContent, dataBuffer, modified, bucketData: data };
}

module.exports = {
    _decodeURI,
    receiveData,
    respondWithData,
    getResponseHeader,
    buildHeadXML,
    buildXML,
    validPath,
    isSystemXML,
    getFileToBuild,
    fetchCapacityMetrics,
    buildVeeamFileData,
    getBucket,
};
