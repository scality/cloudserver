const V4Transform = require('../../../auth/streamingV4/V4Transform');

/**
 * Prepares the stream if the chunks are sent in a v4 Auth request
 * @param {object} stream - stream containing the data
 * @param {object | null } streamingV4Params - if v4 auth, object containing
 * accessKey, signatureFromRequest, region, scopeDate, timestamp, and
 * credentialScope (to be used for streaming v4 auth if applicable)
 * @param {RequestLogger} log - the current request logger
 * @param {function} errCb - callback called if an error occurs
 * @return {object|null} - V4Transform object if v4 Auth request, or
 * the original stream, or null if the request has no V4 params but
 * the type of request requires them
 */
function prepareStream(stream, streamingV4Params, log, errCb) {
    if (stream.headers['x-amz-content-sha256'] ===
        'STREAMING-AWS4-HMAC-SHA256-PAYLOAD') {
        if (typeof streamingV4Params !== 'object') {
            // this might happen if the user provided a valid V2
            // Authentication header, while the chunked upload method
            // requires V4: in such case we don't get any V4 params
            // and we should return an error to the client.
            return null;
        }
        const v4Transform = new V4Transform(streamingV4Params, log, errCb);
        stream.pipe(v4Transform);
        return v4Transform;
    }
    return stream;
}

module.exports = {
    prepareStream,
};
