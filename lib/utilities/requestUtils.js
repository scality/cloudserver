/**
 * getClientIp - Gets the client IP from the request
 * @param {object} request - http request object
 * @return {string} - returns client IP from the request
 */
function getClientIp(request) {
    /**
     * https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/X-Forwarded-For
     *
     * X-Forwarded-For: <client>, <proxy1>, <proxy2>
     *
     * If a request goes through multiple proxies,
     * the IP addresses of each successive proxy is listed.
     * This means, the right-most IP address is the IP address
     * of the most recent proxy and the left-most IP address is
     * the IP address of the originating client.
    */
    return (request.headers['X-Forwarded-For'] ||
        request.headers['x-forwarded-for'] || '').split(',')[0].trim() ||
        request.socket.remoteAddress;
}

module.exports = {
    getClientIp,
};
