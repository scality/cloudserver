import api from '../api/api';
import routesUtils from './routesUtils';

export default function routeHEAD(request, response, log) {
    log.info('received request', { method: 'routeHEAD' });
    // HEAD bucket
    if (request.objectKey === undefined) {
        api.callApiMethod('bucketHead', request, log, (err, resHeaders) =>
            routesUtils.responseNoBody(err, resHeaders, response, 200, log,
                                       routesUtils.onRequestEnd(request)));
    } else {
        // HEAD object
        api.callApiMethod('objectHead', request, log, (err, resHeaders) => {
            const overrideHeaders = {};
            routesUtils.responseContentHeaders(err, overrideHeaders, resHeaders,
                    response, log, routesUtils.onRequestEnd(request));
        });
    }
}
