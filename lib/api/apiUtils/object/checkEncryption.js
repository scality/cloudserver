/**
 * Checks the object encryption request against bucket encryption for matching
 * SSE-S3 configuration
 * @param {object} objectRequest - http request
 * @param {object} serverSideEncryption - bucket encryption info
 * @return {boolean} returns true if the object request has correct SSE-S3
 * configuration
 */
function isValidSSES3(objectRequest, serverSideEncryption) {
    // x-amz-server-side-encryption is allowed only if bucket
    // encryption is enabled and if the value is AES256
    // NOTE: object level encryption is not supported, but we allow
    // encryption headers in the object request headers!
    const sseHeader = objectRequest.headers['x-amz-server-side-encryption'];
    const encryptionAlgorithm = 'AES256';

    const result = ((!serverSideEncryption && sseHeader) ||
        (serverSideEncryption && sseHeader
            && sseHeader === encryptionAlgorithm
            && serverSideEncryption.algorithm !== encryptionAlgorithm) ||
        (serverSideEncryption && sseHeader
            && sseHeader !== encryptionAlgorithm
            && serverSideEncryption.algorithm === encryptionAlgorithm));
    return !result;
}

module.exports = {
    isValidSSES3,
};
