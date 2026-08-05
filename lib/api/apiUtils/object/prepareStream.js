const V4Transform = require('../../../auth/streamingV4/V4Transform');
const TrailingChecksumTransform = require('../../../auth/streamingV4/trailingChecksumTransform');
const ChecksumTransform = require('../../../auth/streamingV4/ChecksumTransform');
const ContentSHA256Transform = require('../../../auth/streamingV4/ContentSHA256Transform');
const { parseContentSHA256, ContentSHA256Type, areChecksumsEnabled } = require('../integrity/validateChecksums');
const { errors, errorInstances, jsutil } = require('arsenal');
const { unsupportedSignatureChecksums } = require('../../../../constants');

/**
 * Instantiates a ChecksumTransform for the given checksum configuration, or
 * returns null if no checksum is requested.
 *
 * @param {object|null} checksum - { algorithm, isTrailer, expected }, or null
 * @param {function} onStreamError - error listener for the transform
 * @param {RequestLogger} log - request logger
 * @return {ChecksumTransform|null} the transform, or null if no checksum
 */
function createChecksumStream(checksum, onStreamError, log) {
    if (!checksum) {
        return null;
    }
    const checksumStream = new ChecksumTransform(checksum.algorithm, checksum.expected, checksum.isTrailer, log);
    checksumStream.on('error', onStreamError);
    return checksumStream;
}

/**
 * Appends the requested checksum transforms to the pipeline: the secondary
 * (only validated) first, then the primary, so that the primary always ends the
 * pipeline and its digest covers the whole body. Either may be absent.
 *
 * @param {stream.Readable} inputStream - stream to append the transforms to
 * @param {object|null} primary - primary checksum ({ algorithm, isTrailer,
 * expected }), or null to compute no stored checksum
 * @param {object|null} secondary - secondary checksum, or null
 * @param {function} onStreamError - error listener for the transforms
 * @param {RequestLogger} log - request logger
 * @return {{ stream: stream.Readable, primaryChecksumStream:
 *   ChecksumTransform|null, secondaryChecksumStream: ChecksumTransform|null }}
 */
function pipeChecksumStreams(inputStream, primary, secondary, onStreamError, log) {
    let stream = inputStream;
    const secondaryChecksumStream = createChecksumStream(secondary, onStreamError, log);
    if (secondaryChecksumStream) {
        stream = stream.pipe(secondaryChecksumStream);
    }
    const primaryChecksumStream = createChecksumStream(primary, onStreamError, log);
    if (primaryChecksumStream) {
        stream = stream.pipe(primaryChecksumStream);
    }
    return { stream, primaryChecksumStream, secondaryChecksumStream };
}

/**
 * Prepares the request stream for data storage by wrapping it in the
 * appropriate transform pipeline based on the x-amz-content-sha256 header.
 * The primary ChecksumTransform (the stored checksum) is the last transform of
 * the pipeline and is returned as primaryChecksumStream; when a secondary
 * checksum is requested it is inserted upstream of the primary and exposed via
 * secondaryChecksumStream.  Callers that need no checksum computed (the
 * Backbeat routes, which rely on content-md5) pass no checksums at all, and
 * get a pipeline without any ChecksumTransform.
 *
 * @param {object} request - incoming HTTP request with headers and body stream
 * @param {object|null} streamingV4Params - v4 streaming auth params (accessKey,
 * signatureFromRequest, region, scopeDate, timestamp, credentialScope), or
 * null/undefined for non-v4 requests
 * @param {object|null} checksums - checksum configuration, or null to compute
 * no checksum at all
 * @param {object|null} checksums.primary - primary checksum
 *   ({ algorithm, isTrailer, expected }) — validated and its digest returned
 * @param {object|null} checksums.secondary - optional secondary checksum
 *   ({ algorithm, isTrailer, expected }) — only validated; used for MPU parts
 * @param {RequestLogger} log - request logger
 * @param {function} errCb - error callback invoked if a stream error occurs
 * @return {{ error: Arsenal.Error|null, stream: stream.Readable|null,
 *   primaryChecksumStream: ChecksumTransform|null,
 *   secondaryChecksumStream: ChecksumTransform|null,
 *   contentSHA256Stream: ContentSHA256Transform|null }}
 */
function prepareStream(request, streamingV4Params, checksums, log, errCb) {
    const xAmzContentSHA256 = request.headers['x-amz-content-sha256'];
    const { primary = null, secondary = null } = (areChecksumsEnabled() && checksums) || {};

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

            return {
                error: null,
                ...pipeChecksumStreams(v4Transform, primary, secondary, onStreamError, log),
                contentSHA256Stream: null,
            };
        }
        case 'STREAMING-UNSIGNED-PAYLOAD-TRAILER': {
            const onStreamError = jsutil.once(errCb);
            const trailingChecksumTransform = new TrailingChecksumTransform(log);
            trailingChecksumTransform.on('error', onStreamError);
            request.pipe(trailingChecksumTransform);
            trailingChecksumTransform.headers = request.headers;

            const checksumStreams = pipeChecksumStreams(
                trailingChecksumTransform,
                primary,
                secondary,
                onStreamError,
                log,
            );
            // The trailer is validated against the secondary checksum when
            // there is one, otherwise against the primary.
            const trailerChecksumStream =
                checksumStreams.secondaryChecksumStream || checksumStreams.primaryChecksumStream;
            if (trailerChecksumStream) {
                trailingChecksumTransform.on('trailer', (name, value) => {
                    trailerChecksumStream.setExpectedChecksum(name, value);
                });
            }
            return { error: null, ...checksumStreams, contentSHA256Stream: null };
        }
        case 'UNSIGNED-PAYLOAD': // Fallthrough
        default: {
            if (unsupportedSignatureChecksums.has(xAmzContentSHA256)) {
                return {
                    error: errorInstances.BadRequest.customizeDescription(`${xAmzContentSHA256} is not supported`),
                    stream: null,
                };
            }

            const parsedContentSHA256 = parseContentSHA256(request.headers);
            const shouldValidateContentSHA256 = parsedContentSHA256.type === ContentSHA256Type.HexSHA256;
            const onStreamError = jsutil.once(errCb);
            let contentSHA256Stream = null;
            let stream = request;
            if (shouldValidateContentSHA256) {
                contentSHA256Stream = new ContentSHA256Transform(parsedContentSHA256.value, log);
                contentSHA256Stream.on('error', onStreamError);
                stream = stream.pipe(contentSHA256Stream);
            }
            return {
                error: null,
                ...pipeChecksumStreams(stream, primary, secondary, onStreamError, log),
                contentSHA256Stream,
            };
        }
    }
}

module.exports = { prepareStream };
