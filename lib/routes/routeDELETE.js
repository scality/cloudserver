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
        api.callApiMethod('multipartDelete', request, response, log,
            (err, corsHeaders) => {
                statsReport500(err, statsClient);
                return routesUtils.responseNoBody(err, corsHeaders, response,
                    204, log);
            });
    } else {
        if (request.objectKey === undefined) {
            if (request.query.website !== undefined) {
                return api.callApiMethod('bucketDeleteWebsite', request,
                response, log, (err, corsHeaders) => {
                    statsReport500(err, statsClient);
                    return routesUtils.responseNoBody(err, corsHeaders,
                        response, 204, log);
                });
            } else if (request.query.cors !== undefined) {
                return api.callApiMethod('bucketDeleteCors', request, response,
                log, (err, corsHeaders) => {
                    statsReport500(err, statsClient);
                    return routesUtils.responseNoBody(err, corsHeaders,
                        response, 204, log);
                });
            }
            api.callApiMethod('bucketDelete', request, response, log,
            (err, corsHeaders) => {
                statsReport500(err, statsClient);
                return routesUtils.responseNoBody(err, corsHeaders, response,
                  204, log);
            });
        } else {
            if (request.query.tagging !== undefined) {
                return api.callApiMethod('objectDeleteTagging', request,
                response, log, (err, resHeaders) => {
                    statsReport500(err, statsClient);
                    return routesUtils.responseNoBody(err, resHeaders,
                        response, 204, log);
                });
            }
            api.callApiMethod('objectDelete', request, response, log,
              (err, corsHeaders) => {
                  /*
                  * Since AWS expects a 204 regardless of the existence of
                  the object, the errors NoSuchKey and NoSuchVersion should not
                  * be sent back as a response.
                  */
                  if (err && !err.NoSuchKey && !err.NoSuchVersion) {
                      return routesUtils.responseNoBody(err, corsHeaders,
                        response, null, log);
                  }
                  statsReport500(err, statsClient);
                  return routesUtils.responseNoBody(null, corsHeaders, response,
                    204, log);
              });
        }
    }
    return undefined;
}
