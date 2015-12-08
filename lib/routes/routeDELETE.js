import utils from '../utils';
import api from '../api/api';
import routesUtils from './routesUtils';

export default function routeDELETE(request, response) {
    utils.normalizeRequest(request);
    // DELETE bucket
    if (utils.getResourceNames(request).object === undefined) {
        api.callApiMethod('bucketDelete', request, (err, resHeaders) =>
            routesUtils.responseNoBody(err, resHeaders, response));
    } else {
        // DELETE multipart
        if (request.query.uploadId) {
            api.callApiMethod('multipartDelete', request, (err, resHeaders) =>
                routesUtils.responseNoBody(err, resHeaders, response));
        } else {
            // DELETE object
            api.callApiMethod('objectDelete', request, (err, resHeaders) =>
                routesUtils.responseNoBody(err, resHeaders, response));
        }
    }
}
