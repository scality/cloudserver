import api from '../api/api';
import routesUtils from './routesUtils';

export default function routeDELETE(request, response, log) {
    // DELETE bucket
    if (request.objectKey === undefined) {
        log.info(`Received DELETE Bucket: ${request.url}`);
        api.callApiMethod('bucketDelete', request, log, (err, resHeaders) =>
            routesUtils.responseNoBody(err, resHeaders, response, 204, log));
    } else {
        // DELETE multipart
        if (request.query.uploadId) {
            log.info(`Received DELETE Multipart: {$request.url}`);
            api.callApiMethod('multipartDelete', request, log,
            (err, resHeaders) =>
                routesUtils.responseNoBody(err, resHeaders, response, 204,
                    log));
        } else {
            // DELETE object
            log.info(`Received DELETE Object: {$request.url}`);
            api.callApiMethod('objectDelete', request, log, (err, resHeaders) =>
                routesUtils.responseNoBody(err, resHeaders, response, 204,
                    log));
        }
    }
}
