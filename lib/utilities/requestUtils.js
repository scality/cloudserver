const { config } = require('../Config');
const { ipCheck } = require('arsenal');

/**
 * getClientIp - Gets the client IP from the request
 * @param {object} request - http request object
 * @param {object} s3config - (optional) s3 config
 * @return {string} - returns client IP from the request
 */
function getClientIp(request, s3config) {
    const clientIp = (request.connection && request.connection.remoteAddress) ||
        (request.socket && request.socket.remoteAddress) ||
        (request.connection && request.connection.socket &&
            request.connection.socket.remoteAddress);

    const requestConfig = (s3config || config).requests;
    if (requestConfig && requestConfig.viaProxy) {
        /**
         * if requests are configured to come via proxy,
         * check from config which proxies are to be trusted and
         * which header to be used to extract client IP
         */
        if (ipCheck.ipMatchCidrList(requestConfig.trustedProxyCIDRs,
                clientIp)) {
            return request.headers[requestConfig.extractClientIPFromHeader]
                .split(',')[0].trim();
        }
    }

    return clientIp;
}

module.exports = {
    getClientIp,
};
