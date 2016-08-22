import api from '../api/api';
import { errors } from 'arsenal';
import routesUtils from './routesUtils';

function _pushMetrics(err, utapi, action, resource) {
    if (!err) {
        const timestamp = Date.now();
        if (action === 'bucketHead') {
            utapi.pushMetricHeadBucket(resource, timestamp);
        } else if (action === 'objectHead') {
            utapi.pushMetricHeadObject(resource, timestamp);
        }
    }
}

export default function routeHEAD(request, response, log, utapi) {
    log.debug('routing request', { method: 'routeHEAD' });
    if (request.bucketName === undefined) {
        log.trace('head request without bucketName');
        routesUtils.responseXMLBody(errors.MethodNotAllowed,
            null, response, log);
    } else if (request.objectKey === undefined) {
        // HEAD bucket
        api.callApiMethod('bucketHead', request, log, (err, resHeaders) => {
            _pushMetrics(err, utapi, 'bucketHead', request.bucketName);
            return routesUtils.responseNoBody(err, resHeaders, response, 200,
                log);
        });
    } else {
        // HEAD object
        api.callApiMethod('objectHead', request, log, (err, resHeaders) => {
            _pushMetrics(err, utapi, 'objectHead', request.bucketName);
            return routesUtils.responseContentHeaders(err, {}, resHeaders,
                                               response, log);
        });
    }
}
