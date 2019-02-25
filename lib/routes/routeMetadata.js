const { errors } = require('arsenal');
const { responseJSONBody } = require('arsenal').s3routes.routesUtils;

function routeMetadata(clientIP, request, response, log) {
    log.debug('routing request', { method: 'routeMetadata' });
    return responseJSONBody(errors.NotImplemented, null, response, log);
}

module.exports = routeMetadata;
