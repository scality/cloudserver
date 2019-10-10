const { ipCheck } = require('arsenal');
const { config } = require('../Config');

/**
 * getClientIp - Gets the client IP from the request
 * @param {object} request - http request object
 * @param {object} s3config - (optional) s3 config
 * @return {string} - returns client IP from the request
 */
function getClientIp(request, s3config) {
    const clientIp = request.socket.remoteAddress;
    const requestConfig = (s3config || config).requests;
    if (requestConfig && requestConfig.viaProxy) {
        /**
         * if requests are configured to come via proxy,
         * check from config which proxies are to be trusted and
         * which header to be used to extract client IP
         */
        if (ipCheck.ipMatchCidrList(requestConfig.trustedProxyCIDRs,
            clientIp)) {
            const ipFromHeader
            // eslint-disable-next-line operator-linebreak
                = request.headers[requestConfig.extractClientIPFromHeader];
            if (ipFromHeader && ipFromHeader.trim().length) {
                return ipFromHeader.split(',')[0].trim();
            }
        }
    }

    return clientIp;
}

module.exports = {
    getClientIp,
};
