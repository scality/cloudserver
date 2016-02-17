import api from '../api/api';
import routesUtils from './routesUtils';

export default function routeDELETE(request, response, log) {
    log.info('received request', { method: 'routeDELETE' });

    if (request.objectKey === undefined) {
        api.callApiMethod('bucketDelete', request, log, (err, resHeaders) =>
            routesUtils.responseNoBody(err, resHeaders, response, 204, log,
                        routesUtils.onRequestEnd(request)));
    } else {
        if (request.query.uploadId) {
            api.callApiMethod('multipartDelete', request, log,
                (err, resHeaders) =>
                    routesUtils.responseNoBody(err, resHeaders, response, 204,
                        log, routesUtils.onRequestEnd(request))
            );
        } else {
            api.callApiMethod('objectDelete', request, log, (err, resHeaders) =>
                    routesUtils.responseNoBody(err, resHeaders, response, 204,
                        log, routesUtils.onRequestEnd(request))
            );
        }
    }
}
