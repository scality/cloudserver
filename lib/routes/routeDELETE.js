import api from '../api/api';
import routesUtils from './routesUtils';

export default function routeDELETE(request, response, log) {
    log.debug('routing request', { method: 'routeDELETE' });

    if (request.objectKey === undefined) {
        api.callApiMethod('bucketDelete', request, log, (err, resHeaders) =>
            routesUtils.responseNoBody(err, resHeaders, response, 204, log));
    } else {
        if (request.query.uploadId) {
            api.callApiMethod('multipartDelete', request, log,
                (err, resHeaders) =>
                    routesUtils.responseNoBody(err, resHeaders, response, 204,
                        log)
            );
        } else {
            api.callApiMethod('objectDelete', request, log,
              (err, resHeaders) => {
                  /*
                  * Since AWS expects a 204 regardless of the existence of the
                  * object, the error NoSuchKey should not be sent back as a
                  * response.
                  */
                  if (err && !err.NoSuchKey) {
                      return routesUtils.responseNoBody(err, resHeaders,
                        response, null, log);
                  }
                  return routesUtils.responseNoBody(null, resHeaders, response,
                    204, log);
              });
        }
    }
}
