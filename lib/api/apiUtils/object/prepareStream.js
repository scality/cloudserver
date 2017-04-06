const V4Transform = require('../../../auth/streamingV4/V4Transform');

/**
 * Prepares the stream if the chunks are sent in a v4 Auth request
 * @param {object} stream - stream containing the data
 * @param {object | null } streamingV4Params - if v4 auth, object containing
 * accessKey, signatureFromRequest, region, scopeDate, timestamp, and
 * credentialScope (to be used for streaming v4 auth if applicable)
 * @param {RequestLogger} log - the current request logger
 * @param {function} cb - callback containing the result for V4Transform
 * @return {object} - V4Transform object if v4 Auth request, or else the stream
 */
function prepareStream(stream, streamingV4Params, log, cb) {
    if (stream.headers['x-amz-content-sha256'] ===
        'STREAMING-AWS4-HMAC-SHA256-PAYLOAD') {
        const v4Transform = new V4Transform(streamingV4Params, log, cb);
        stream.pipe(v4Transform);
        return v4Transform;
    }
    return stream;
}

module.exports = {
    prepareStream,
};
