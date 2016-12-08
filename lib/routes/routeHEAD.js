import api from '../api/api';
import { errors } from 'arsenal';
import routesUtils from './routesUtils';
import statsReport500 from '../utilities/statsReport500';

export default function routeHEAD(request, response, log, statsClient) {
    log.debug('routing request', { method: 'routeHEAD' });
    if (request.bucketName === undefined) {
        log.trace('head request without bucketName');
        routesUtils.responseXMLBody(errors.MethodNotAllowed,
            null, response, log);
    } else if (request.objectKey === undefined) {
        // HEAD bucket
        api.callApiMethod('bucketHead', request, log, (err, resHeaders) => {
            statsReport500(err, statsClient);
            return routesUtils.responseNoBody(err, resHeaders, response, 200,
                log);
        });
    } else {
        // HEAD object
        api.callApiMethod('objectHead', request, log, (err, resHeaders) => {
            statsReport500(err, statsClient);
            return routesUtils.responseContentHeaders(err, {}, resHeaders,
                                               response, log);
        });
    }
}
