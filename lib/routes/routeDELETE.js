import api from '../api/api';
import routesUtils from './routesUtils';

function _pushMetrics(err, utapi, action, resource) {
    if (!err) {
        const timestamp = Date.now();
        if (action === 'bucketDelete') {
            utapi.pushMetricDeleteBucket(resource, timestamp);
        } else if (action === 'objectDelete') {
            utapi.pushMetricDeleteObject(resource, timestamp);
        } else if (action === 'multipartDelete') {
            utapi.pushMetricAbortMultipartUpload(resource, timestamp);
        }
    }
}

export default function routeDELETE(request, response, log, utapi) {
    log.debug('routing request', { method: 'routeDELETE' });

    if (request.objectKey === undefined) {
        api.callApiMethod('bucketDelete', request, log, (err, resHeaders) => {
            _pushMetrics(err, utapi, 'bucketDelete', request.bucketName);
            return routesUtils.responseNoBody(err, resHeaders, response, 204,
                log);
        });
    } else {
        if (request.query.uploadId) {
            api.callApiMethod('multipartDelete', request, log,
                (err, resHeaders) => {
                    _pushMetrics(err, utapi, 'multipartDelete',
                        request.bucketName);
                    return routesUtils.responseNoBody(err, resHeaders, response,
                        204, log);
                });
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
                  _pushMetrics(err, utapi, 'objectDelete', request.bucketName);
                  return routesUtils.responseNoBody(null, resHeaders, response,
                    204, log);
              });
        }
    }
}
