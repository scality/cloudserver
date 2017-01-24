import api from '../api/api';
import routesUtils from './routesUtils';
import statsReport500 from '../utilities/statsReport500';
import { errors } from 'arsenal';


export default function routeDELETE(request, response, log, statsClient) {
    log.debug('routing request', { method: 'routeDELETE' });

    if (request.query.uploadId) {
        if (request.objectKey === undefined) {
            return routesUtils.responseNoBody(
              errors.InvalidRequest.customizeDescription('A key must be ' +
              'specified'), null, response, 200, log);
        }
        api.callApiMethod('multipartDelete', request, log,
            (err, resHeaders) => {
                statsReport500(err, statsClient);
                return routesUtils.responseNoBody(err, resHeaders, response,
                    204, log);
            });
    } else {
        if (request.objectKey === undefined) {
            if (request.query.website !== undefined) {
                return api.callApiMethod('bucketDeleteWebsite', request,
                log, err => {
                    statsReport500(err, statsClient);
                    return routesUtils.responseNoBody(err, null, response, 204,
                        log);
                });
            } else if (request.query.cors !== undefined) {
                return api.callApiMethod('bucketDeleteCors', request, log,
                err => {
                    statsReport500(err, statsClient);
                    return routesUtils.responseNoBody(err, null, response, 204,
                        log);
                });
            }
            api.callApiMethod('bucketDelete', request, log,
            (err, resHeaders) => {
                statsReport500(err, statsClient);
                return routesUtils.responseNoBody(err, resHeaders, response,
                  204, log);
            });
        } else {
            api.callApiMethod('objectDelete', request, log,
              err => {
                  /*
                  * Since AWS expects a 204 regardless of the existence of
                  the object, the error NoSuchKey should not be sent back
                  * as a response.
                  */
                  if (err && !err.NoSuchKey) {
                      return routesUtils.responseNoBody(err, null,
                        response, null, log);
                  }
                  statsReport500(err, statsClient);
                  return routesUtils.responseNoBody(null, null, response,
                    204, log);
              });
        }
    }
    return undefined;
}
