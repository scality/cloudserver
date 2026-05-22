const V4Transform = require('../../../auth/streamingV4/V4Transform');
const TrailingChecksumTransform = require('../../../auth/streamingV4/trailingChecksumTransform');
const ChecksumTransform = require('../../../auth/streamingV4/ChecksumTransform');
const { errors, errorInstances, jsutil } = require('arsenal');
const { unsupportedSignatureChecksums } = require('../../../../constants');

/**
 * Prepares the request stream for data storage by wrapping it in the
 * appropriate transform pipeline based on the x-amz-content-sha256 header.
 * The returned stream is always the primary ChecksumTransform (the stored
 * checksum).  When a secondary checksum is requested it is inserted upstream
 * of the primary and exposed via secondaryChecksumStream.
 *
 * @param {object} request - incoming HTTP request with headers and body stream
 * @param {object|null} streamingV4Params - v4 streaming auth params (accessKey,
 * signatureFromRequest, region, scopeDate, timestamp, credentialScope), or
 * null/undefined for non-v4 requests
 * @param {object} checksums - checksum configuration
 * @param {object} checksums.primary - primary checksum
 *   ({ algorithm, isTrailer, expected }) — validated and its digest returned
 * @param {object|null} checksums.secondary - optional secondary checksum
 *   ({ algorithm, isTrailer, expected }) — only validated; used for MPU parts
 * @param {RequestLogger} log - request logger
 * @param {function} errCb - error callback invoked if a stream error occurs
 * @return {{ error: Arsenal.Error|null, stream: ChecksumTransform|null,
 *   secondaryChecksumStream: ChecksumTransform|null }}
 */
function prepareStream(request, streamingV4Params, checksums, log, errCb) {
    const xAmzContentSHA256 = request.headers['x-amz-content-sha256'];
    const { primary, secondary } = checksums;

    switch (xAmzContentSHA256) {
        case 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD': {
            if (streamingV4Params === null || typeof streamingV4Params !== 'object') {
                // this might happen if the user provided a valid V2
                // Authentication header, while the chunked upload method
                // requires V4: in such case we don't get any V4 params
                // and we should return an error to the client.
                log.error('missing v4 streaming params for chunked upload', {
                    method: 'prepareStream',
                    streamingV4ParamsType: typeof streamingV4Params,
                    streamingV4Params,
                });
                return { error: errors.InvalidArgument, stream: null };
            }
            // Use a once-guard so that if both V4Transform and ChecksumTransform
            // independently error, errCb is only called once.
            const onStreamError = jsutil.once(errCb);
            const v4Transform = new V4Transform(streamingV4Params, log, onStreamError);
            request.pipe(v4Transform);
            v4Transform.headers = request.headers;

            let secondaryChecksumStream = null;
            let stream = v4Transform;
            if (secondary) {
                secondaryChecksumStream = new ChecksumTransform(
                    secondary.algorithm,
                    secondary.expected,
                    secondary.isTrailer,
                    log,
                );
                secondaryChecksumStream.on('error', onStreamError);
                stream = v4Transform.pipe(secondaryChecksumStream);
            }

            const primaryStream = new ChecksumTransform(primary.algorithm, primary.expected, primary.isTrailer, log);
            primaryStream.on('error', onStreamError);
            return { error: null, stream: stream.pipe(primaryStream), secondaryChecksumStream };
        }
        case 'STREAMING-UNSIGNED-PAYLOAD-TRAILER': {
            const onStreamError = jsutil.once(errCb);
            const trailingChecksumTransform = new TrailingChecksumTransform(log);
            trailingChecksumTransform.on('error', onStreamError);
            request.pipe(trailingChecksumTransform);
            trailingChecksumTransform.headers = request.headers;

            let secondaryChecksumStream = null;
            let stream = trailingChecksumTransform;
            if (secondary) {
                secondaryChecksumStream = new ChecksumTransform(
                    secondary.algorithm,
                    secondary.expected,
                    secondary.isTrailer,
                    log,
                );
                secondaryChecksumStream.on('error', onStreamError);
                stream = trailingChecksumTransform.pipe(secondaryChecksumStream);
                trailingChecksumTransform.on('trailer', (name, value) => {
                    secondaryChecksumStream.setExpectedChecksum(name, value);
                });
            }

            const primaryStream = new ChecksumTransform(primary.algorithm, primary.expected, primary.isTrailer, log);
            primaryStream.on('error', onStreamError);
            if (!secondary) {
                trailingChecksumTransform.on('trailer', (name, value) => {
                    primaryStream.setExpectedChecksum(name, value);
                });
            }
            return { error: null, stream: stream.pipe(primaryStream), secondaryChecksumStream };
        }
        case 'UNSIGNED-PAYLOAD': // Fallthrough
        default: {
            if (unsupportedSignatureChecksums.has(xAmzContentSHA256)) {
                return {
                    error: errorInstances.BadRequest.customizeDescription(`${xAmzContentSHA256} is not supported`),
                    stream: null,
                };
            }

            const onStreamError = secondary ? jsutil.once(errCb) : errCb;
            let secondaryChecksumStream = null;
            let stream = request;
            if (secondary) {
                secondaryChecksumStream = new ChecksumTransform(
                    secondary.algorithm,
                    secondary.expected,
                    secondary.isTrailer,
                    log,
                );
                secondaryChecksumStream.on('error', onStreamError);
                stream = request.pipe(secondaryChecksumStream);
            }

            const primaryStream = new ChecksumTransform(primary.algorithm, primary.expected, primary.isTrailer, log);
            primaryStream.on('error', onStreamError);
            return { error: null, stream: stream.pipe(primaryStream), secondaryChecksumStream };
        }
    }
}

module.exports = { prepareStream };
