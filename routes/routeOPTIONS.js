const { errors } = require('arsenal');


const api = require('../api/api');
const routesUtils = require('./routesUtils');
const statsReport500 = require('../utilities/statsReport500');

function routeOPTIONS(request, response, log, statsClient) {
    log.debug('routing request', { method: 'routeOPTION' });

    const corsMethod = request.headers['access-control-request-method'] || null;

    if (!request.headers.origin) {
        const msg = 'Insufficient information. Origin request header needed.';
        const err = errors.BadRequest.customizeDescription(msg);
        log.debug('missing origin', { method: 'routeOPTIONS', error: err });
        return routesUtils.responseXMLBody(err, undefined, response, log);
    }
    if (['GET', 'PUT', 'HEAD', 'POST', 'DELETE'].indexOf(corsMethod) < 0) {
        const msg = `Invalid Access-Control-Request-Method: ${corsMethod}`;
        const err = errors.BadRequest.customizeDescription(msg);
        log.debug('invalid Access-Control-Request-Method',
            { method: 'routeOPTIONS', error: err });
        return routesUtils.responseXMLBody(err, undefined, response, log);
    }

    return api.callApiMethod('corsPreflight', request, response, log,
    (err, resHeaders) => {
        statsReport500(err, statsClient);
        return routesUtils.responseNoBody(err, resHeaders, response, 200,
            log);
    });
}

module.exports = routeOPTIONS;
