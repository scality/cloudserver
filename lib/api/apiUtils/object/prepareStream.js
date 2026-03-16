const V4Transform = require('../../../auth/streamingV4/V4Transform');
const TrailingChecksumTransform = require('../../../auth/streamingV4/trailingChecksumTransform');
const ChecksumTransform = require('../../../auth/streamingV4/ChecksumTransform');
const {
    getChecksumDataFromHeaders,
    arsenalErrorFromChecksumError,
} = require('../../apiUtils/integrity/validateChecksums');
const { errors, jsutil } = require('arsenal');
const { unsupportedSignatureChecksums } = require('../../../../constants');

/**
 * Prepares the request stream for data storage by wrapping it in the
 * appropriate transform pipeline based on the x-amz-content-sha256 header.
 * Always returns a ChecksumTransform as the final stream.
 * If no checksum was sent by the client a CRC64NVME ChecksumTransform is returned.
 *
 * @param {object} request - incoming HTTP request with headers and body stream
 * @param {object|null} streamingV4Params - v4 streaming auth params (accessKey,
 * signatureFromRequest, region, scopeDate, timestamp, credentialScope), or
 * null/undefined for non-v4 requests
 * @param {RequestLogger} log - request logger
 * @param {function} errCb - error callback invoked if a stream error occurs
 * @return {{ error: Arsenal.Error|null, stream: ChecksumTransform|null }}
 * error is set and stream is null if the request headers are invalid;
 * otherwise error is null and stream is the ChecksumTransform to read from
 */
function prepareStream(request, streamingV4Params, log, errCb) {
    const xAmzContentSHA256 = request.headers['x-amz-content-sha256'];

    const checksumAlgo = getChecksumDataFromHeaders(request.headers);
    if (checksumAlgo.error) {
        log.debug('prepareStream invalid checksum headers', checksumAlgo);
        return { error: arsenalErrorFromChecksumError(checksumAlgo), stream: null };
    }

    let stream = request;
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
            stream = v4Transform;

            const checksumedStream = new ChecksumTransform(
                checksumAlgo.algorithm,
                checksumAlgo.expected,
                checksumAlgo.isTrailer,
                log,
            );
            checksumedStream.on('error', onStreamError);
            stream.pipe(checksumedStream);
            return { error: null, stream: checksumedStream };
        }
        case 'STREAMING-UNSIGNED-PAYLOAD-TRAILER': {
            // Use a once-guard so that auto-destroying both piped streams
            // when one errors does not result in errCb being called twice.
            const onStreamError = jsutil.once(errCb);
            const trailingChecksumTransform = new TrailingChecksumTransform(log);
            trailingChecksumTransform.on('error', onStreamError);
            request.pipe(trailingChecksumTransform);
            trailingChecksumTransform.headers = request.headers;
            stream = trailingChecksumTransform;

            const checksumedStream = new ChecksumTransform(
                checksumAlgo.algorithm,
                checksumAlgo.expected,
                checksumAlgo.isTrailer,
                log,
            );
            checksumedStream.on('error', onStreamError);
            trailingChecksumTransform.on('trailer', (name, value) => {
                checksumedStream.setExpectedChecksum(name, value);
            });
            stream.pipe(checksumedStream);
            return { error: null, stream: checksumedStream };
        }
        case 'UNSIGNED-PAYLOAD': // Fallthrough
        default: {
            if (unsupportedSignatureChecksums.has(xAmzContentSHA256)) {
                return {
                    error: errors.BadRequest.customizeDescription(`${xAmzContentSHA256} is not supported`),
                    stream: null,
                };
            }

            const checksumedStream = new ChecksumTransform(
                checksumAlgo.algorithm,
                checksumAlgo.expected,
                checksumAlgo.isTrailer,
                log,
            );
            checksumedStream.on('error', errCb);
            stream.pipe(checksumedStream);
            return { error: null, stream: checksumedStream };
        }
    }
}

module.exports = { prepareStream };
