import api from '../api/api';
import { errors } from 'arsenal';
import routesUtils from './routesUtils';

export default function routeHEAD(request, response, log) {
    log.debug('routing request', { method: 'routeHEAD' });
    if (request.bucketName === undefined) {
        log.trace('head request without bucketName');
        routesUtils.responseXMLBody(errors.MethodNotAllowed,
            null, response, log);
    } else if (request.objectKey === undefined) {
        // HEAD bucket
        api.callApiMethod('bucketHead', request, log, (err, resHeaders) =>
            routesUtils.responseNoBody(err, resHeaders, response, 200, log));
    } else {
        // HEAD object
        api.callApiMethod('objectHead', request, log, (err, resHeaders) => {
            routesUtils.responseContentHeaders(err, {}, resHeaders,
                                               response, log);
        });
    }
}
