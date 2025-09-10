const crypto = require('crypto');

/**
 * validateChecksumsNoChunking - Validate the checksums of a request.
 * @param {object} headers - http headers
 * @param {string} body - http request body
 * @return {string} - error message
 */
function validateChecksumsNoChunking(headers, body) {
    if (headers['content-md5']) {
        const md5 = crypto.createHash('md5').update(body, 'utf8').digest('base64');
        if (md5 !== headers['content-md5']) {
            return `md5 mismatch, calculated ${md5} expected ${headers['content-md5']}`;
        }
    }

    return null;
}

module.exports = { validateChecksumsNoChunking };
