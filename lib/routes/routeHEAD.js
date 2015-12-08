import utils from '../utils';
import api from '../api/api';
import routesUtils from './routesUtils';

export default function routeHEAD(request, response) {
    utils.normalizeRequest(request);
    // HEAD bucket
    if (utils.getResourceNames(request).object === undefined) {
        api.callApiMethod('bucketHead', request, (err, resHeaders) =>
            routesUtils.responseNoBody(err, resHeaders, response));
    } else {
        // HEAD object
        api.callApiMethod('objectHead', request, (err, resHeaders) =>
            routesUtils.responseContentHeaders(err, resHeaders, request,
                    response));
    }
}
