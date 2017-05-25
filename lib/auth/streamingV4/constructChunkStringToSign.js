const crypto = require('crypto');

const constants = require('../../../constants');

/**
 * Constructs stringToSign for chunk
 * @param {string} timestamp - date parsed from headers
 * in ISO 8601 format: YYYYMMDDTHHMMSSZ
 * @param {string} credentialScope - items from auth
 * header plus the string 'aws4_request' joined with '/':
 * timestamp/region/aws-service/aws4_request
 * @param {string} lastSignature - signature from headers or prior chunk
 * @param {string} justDataChunk - data portion of chunk
 * @returns {string} stringToSign
 */
function constructChunkStringToSign(timestamp,
    credentialScope, lastSignature, justDataChunk) {
    let currentChunkHash;
    // for last chunk, there will be no data, so use emptyStringHash
    if (!justDataChunk) {
        currentChunkHash = constants.emptyStringHash;
    } else {
        currentChunkHash = crypto.createHash('sha256');
        currentChunkHash = currentChunkHash
            .update(justDataChunk, 'binary').digest('hex');
    }
    return `AWS4-HMAC-SHA256-PAYLOAD\n${timestamp}\n` +
        `${credentialScope}\n${lastSignature}\n` +
        `${constants.emptyStringHash}\n${currentChunkHash}`;
}

module.exports = constructChunkStringToSign;
