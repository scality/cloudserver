/**
 * Decodes an URI and return the result.
 * Do the same decoding than in S3 server
 * @param {string} uri - uri to decode
 * @returns {string} -
 */
function _decodeURI(uri) {
    return decodeURIComponent(uri.replace(/\+/g, ' '));
}

const validPath = '.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/';

module.exports = {
    _decodeURI,
    validPath,
};
