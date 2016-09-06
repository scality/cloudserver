import api from '../api/api';
import routesUtils from './routesUtils';

function _pushMetrics(err, log, utapi, action, resource, contentLength) {
    if (!err) {
        const timestamp = Date.now();
        const reqUid = log.getSerializedUids();
        if (action === 'bucketDelete') {
            utapi.pushMetricDeleteBucket(reqUid, resource, timestamp);
        } else if (action === 'objectDelete') {
            utapi.pushMetricDeleteObject(reqUid, resource, timestamp,
                contentLength);
        } else if (action === 'multipartDelete') {
            utapi.pushMetricAbortMultipartUpload(reqUid, resource, timestamp);
        }
    }
}

export default function routeDELETE(request, response, log, utapi) {
    log.debug('routing request', { method: 'routeDELETE' });

    if (request.objectKey === undefined) {
        api.callApiMethod('bucketDelete', request, log, (err, resHeaders) => {
            _pushMetrics(err, log, utapi, 'bucketDelete', request.bucketName);
            return routesUtils.responseNoBody(err, resHeaders, response, 204,
                log);
        });
    } else {
        if (request.query.uploadId) {
            api.callApiMethod('multipartDelete', request, log,
                (err, resHeaders) => {
                    _pushMetrics(err, log, utapi, 'multipartDelete',
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
                  _pushMetrics(err, log, utapi, 'objectDelete',
                    request.bucketName, contentLength);
                  return routesUtils.responseNoBody(null, null, response,
                    204, log);
              });
        }
    }
}
