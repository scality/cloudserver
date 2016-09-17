import api from '../api/api';
import { errors } from 'arsenal';
import routesUtils from './routesUtils';
import pushMetrics from '../utilities/pushMetrics';

export default function routeHEAD(request, response, log, utapi) {
    log.debug('routing request', { method: 'routeHEAD' });
    if (request.bucketName === undefined) {
        log.trace('head request without bucketName');
        routesUtils.responseXMLBody(errors.MethodNotAllowed,
            null, response, log);
    } else if (request.objectKey === undefined) {
        // HEAD bucket
        api.callApiMethod('bucketHead', request, log, (err, resHeaders) => {
            pushMetrics(err, log, utapi, 'bucketHead', request.bucketName);
            return routesUtils.responseNoBody(err, resHeaders, response, 200,
                log);
        });
    } else {
        // HEAD object
        api.callApiMethod('objectHead', request, log, (err, resHeaders) => {
            pushMetrics(err, log, utapi, 'objectHead', request.bucketName);
            return routesUtils.responseContentHeaders(err, {}, resHeaders,
                                               response, log);
        });
    }
}
