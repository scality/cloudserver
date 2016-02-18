import api from '../api/api';
import routesUtils from './routesUtils';

export default function routeDELETE(request, response, log) {
    log.info('received request', { method: 'routeDELETE' });

    // DELETE bucket
    if (request.objectKey === undefined) {
        api.callApiMethod('bucketDelete', request, log, (err, resHeaders) =>
            routesUtils.responseNoBody(err, resHeaders, response, 204, log));
    } else {
        // DELETE multipart
        if (request.query.uploadId) {
            api.callApiMethod('multipartDelete', request, log,
            (err, resHeaders) =>
                routesUtils.responseNoBody(err, resHeaders, response, 204,
                    log));
        } else {
            // DELETE object
            api.callApiMethod('objectDelete', request, log, (err, resHeaders) =>
                routesUtils.responseNoBody(err, resHeaders, response, 204,
                    log));
        }
    }
}
