import utils from '../utils';
import api from '../api/api';
import routesUtils from './routesUtils';

export default function routeHEAD(request, response, log) {
    utils.normalizeRequest(request);
    // HEAD bucket
    if (utils.getResourceNames(request).object === undefined) {
        log.info(`Received HEAD Bucket: ${request.url}`);
        api.callApiMethod('bucketHead', request, log, (err, resHeaders) =>
            routesUtils.responseNoBody(err, resHeaders, response, 200, log));
    } else {
        // HEAD object
        log.info(`Received HEAD Object: ${request.url}`);
        api.callApiMethod('objectHead', request, log, (err, resHeaders) => {
            const overrideHeaders = {};
            routesUtils.responseContentHeaders(err, overrideHeaders, resHeaders,
                    response, log);
        });
    }
}
