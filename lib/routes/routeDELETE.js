import api from '../api/api';
import routesUtils from './routesUtils';
import pushMetrics from '../utilities/pushMetrics';
import statsReport500 from '../utilities/statsReport500';

export default function routeDELETE(request, response, log, utapi,
    statsClient) {
    log.debug('routing request', { method: 'routeDELETE' });

    if (request.objectKey === undefined) {
        api.callApiMethod('bucketDelete', request, log, (err, resHeaders) => {
            statsReport500(err, statsClient);
            pushMetrics(err, log, utapi, 'bucketDelete', request.bucketName);
            return routesUtils.responseNoBody(err, resHeaders, response, 204,
                log);
        });
    } else {
        if (request.query.uploadId) {
            api.callApiMethod('multipartDelete', request, log,
                (err, resHeaders) => {
                    statsReport500(err, statsClient);
                    pushMetrics(err, log, utapi, 'multipartDelete',
                        request.bucketName);
                    return routesUtils.responseNoBody(err, resHeaders, response,
                        204, log);
                });
        } else {
            api.callApiMethod('objectDelete', request, log,
              (err, contentLength) => {
                  /*
                  * Since AWS expects a 204 regardless of the existence of the
                  * object, the error NoSuchKey should not be sent back as a
                  * response.
                  */
                  if (err && !err.NoSuchKey) {
                      return routesUtils.responseNoBody(err, null,
                        response, null, log);
                  }
                  statsReport500(err, statsClient);
                  pushMetrics(err, log, utapi, 'objectDelete',
                    request.bucketName, contentLength);
                  return routesUtils.responseNoBody(null, null, response,
                    204, log);
              });
        }
    }
}
