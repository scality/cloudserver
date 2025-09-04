const { pipeline } = require('stream');
const { jsutil } = require('arsenal');

const V4Transform = require('../../../auth/streamingV4/V4Transform');
const TrailingChecksumTransform = require('../../../auth/streamingV4/trailingChecksumTransform');

// Allow tuning watermarks via env
const RW_HWM_BYTES = Math.max(
    64 * 1024,
    Number.parseInt(process.env.STREAM_HIGH_WATER_MARK_BYTES || '', 10) || 512 * 1024,
);

/**
 * Create an optimized data stream that composes only the needed transforms.
 * Fast path: if neither V4 streaming nor trailer checksums are used, return the
 * original input stream to avoid any extra overhead.
 */
function createOptimizedDataStream(inputStream, streamingV4Params, log, cbOnce) {
    if (!inputStream) {
        const err = new Error('Invalid input stream');
        cbOnce(err);
        return null;
    }

    const sha256Header = inputStream.headers && inputStream.headers['x-amz-content-sha256'];
    const needsV4 = sha256Header === 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD';
    const needsTrailer = sha256Header === 'STREAMING-UNSIGNED-PAYLOAD-TRAILER';

    // Fast path: no transforms
    if (!needsV4 && !needsTrailer) {
        return inputStream;
    }

    const transforms = [];
    if (needsV4) {
        transforms.push(new V4Transform(streamingV4Params, log, jsutil.once(cbOnce), {
            readableHighWaterMark: RW_HWM_BYTES,
            writableHighWaterMark: RW_HWM_BYTES,
        }));
    }
    if (needsTrailer) {
        transforms.push(new TrailingChecksumTransform(log, {
            readableHighWaterMark: RW_HWM_BYTES,
            writableHighWaterMark: RW_HWM_BYTES,
        }));
    }

    // Build a single pipeline for robust backpressure and error propagation
    // Return the last stream in the chain to be consumed by storage.put
    const last = transforms[transforms.length - 1];
    // Preserve headers on the exposed stream as legacy code relies on them
    if (last) {
        last.headers = inputStream.headers;  
    }
    pipeline(inputStream, ...transforms, err => {
        if (err) {
            log?.error('stream pipeline error', { error: err });
            jsutil.once(cbOnce)(err);
        }
    });
    return last;
}

module.exports = {
    createOptimizedDataStream,
};
