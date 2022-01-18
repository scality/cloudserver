const { errors } = require('arsenal');
const { maxHttpHeadersSize } = require('../../../../constants');

/**
 * Checks the size of the size of the HTTP headers
 * @param {object} requestHeaders - HTTP request headers
 * @return {object} object with error or null
 */
function checkHttpHeadersSize(requestHeaders) {
    let httpHeadersSize = 0;

    Object.keys(requestHeaders).forEach(header => {
        httpHeadersSize += Buffer.byteLength(header, 'utf8') +
            Buffer.byteLength(requestHeaders[header], 'utf8');
    });

    if (httpHeadersSize > maxHttpHeadersSize) {
        return {
            httpHeadersSizeError: errors.HttpHeadersTooLarge,
        };
    }
    return {};
}

module.exports = checkHttpHeadersSize;
