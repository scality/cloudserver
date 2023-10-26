const { errors } = require('arsenal');

const { possibleSignatureChecksums, supportedSignatureChecksums } = require('../../../../constants');

function validateChecksumHeaders(headers) {
    // If the x-amz-trailer header is present the request is using one of the
    // trailing checksum algorithms, which are not supported.
    if (headers['x-amz-trailer'] !== undefined) {
        return errors.BadRequest.customizeDescription('trailing checksum is not supported');
    }

    const signatureChecksum = headers['x-amz-content-sha256'];
    if (signatureChecksum === undefined) {
        return null;
    }

    if (supportedSignatureChecksums.has(signatureChecksum)) {
        return null;
    }

    // If the value is not one of the possible checksum algorithms
    // the only other valid value is the actual sha256 checksum of the payload.
    // Do a simple sanity check of the length to guard against future algos.
    // If the value is an unknown algo, then it will fail checksum validation.
    if (!possibleSignatureChecksums.has(signatureChecksum) && signatureChecksum.length === 64) {
        return null;
    }

    return errors.BadRequest.customizeDescription('unsupported checksum algorithm');
}

module.exports = validateChecksumHeaders;
