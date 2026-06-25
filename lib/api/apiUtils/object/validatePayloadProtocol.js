const { errorInstances } = require('arsenal');

const { parseContentSHA256, ContentSHA256Type, errInvalidContentSHA256 } = require('../integrity/validateChecksums');

/**
 * Validate the SigV4 payload protocol selected by x-amz-content-sha256.
 *
 * @param {object} headers - http request headers
 * @return {ArsenalError|null} - error if the protocol is unsupported/malformed, else null
 */
function validatePayloadProtocol(headers) {
    const parsed = parseContentSHA256(headers);
    switch (parsed.type) {
        case ContentSHA256Type.Skip: // not SigV4 header auth; header not meaningful
            return null;
        case ContentSHA256Type.Absent:
            return null;
        case ContentSHA256Type.Unsigned:
            return null;
        case ContentSHA256Type.HexSHA256:
            return null;
        case ContentSHA256Type.Streaming:
            return parsed.supported
                ? null
                : errorInstances.BadRequest.customizeDescription(`${parsed.value} is not supported`);
        case ContentSHA256Type.Invalid:
            return errInvalidContentSHA256;
    }
    return null;
}

module.exports = validatePayloadProtocol;
