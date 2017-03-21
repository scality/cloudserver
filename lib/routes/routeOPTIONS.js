import { errors } from 'arsenal';

import api from '../api/api';
import routesUtils from './routesUtils';
import statsReport500 from '../utilities/statsReport500';

export default function routeOPTIONS(request, response, log, statsClient) {
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
