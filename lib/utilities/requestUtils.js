const { config } = require('../Config');

/**
 * getClientIp - Gets the client IP from the request
 * @param {object} request - http request object
 * @return {string} - returns client IP from the request
 */
function getClientIp(request) {
    const clientIp = (request.connection && request.connection.remoteAddress) ||
        (request.socket && request.socket.remoteAddress) ||
        (request.connection && request.connection.socket &&
        request.connection.socket.remoteAddress);

    const requestConfig = config.requests;
    if (requestConfig && requestConfig.viaProxy) {
        /**
         * if requests are configured to come via proxy,
         * check from config which IPs are to be trusted and
         * which header to be used to extract client IP
        */
        if (requestConfig.trustedProxyIPs.includes(clientIp)) {
            return request.headers[requestConfig.extractClientIPFromHeader]
                .split(',')[0].trim();
        }
    }

    return clientIp;
}

module.exports = {
    getClientIp,
};
