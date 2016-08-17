import { errors } from 'arsenal';

import api from '../api/api';
import routesUtils from './routesUtils';

function _pushMetrics(err, utapi, action, resource) {
    if (!err) {
        const timestamp = Date.now();
        if (action === 'initiateMultipartUpload') {
            utapi.pushMetricInitiateMultipartUpload(resource, timestamp);
        } else if (action === 'completeMultipartUpload') {
            utapi.pushMetricCompleteMultipartUpload(resource, timestamp);
        }
    }
}

export default function routePOST(request, response, log, utapi) {
    log.debug('routing request', { method: 'routePOST' });
    request.post = '';

    request.on('data', chunk => request.post += chunk.toString());

    request.on('end', () => {
        if (request.objectKey === undefined) {
            return routesUtils.responseNoBody(errors.InvalidURI, null, response,
                200, log);
        } else if (request.query.uploads !== undefined) {
            // POST multipart upload
            api.callApiMethod('initiateMultipartUpload', request, log,
                (err, result) => {
                    _pushMetrics(err, utapi, 'initiateMultipartUpload',
                        request.bucketName);
                    return routesUtils.responseXMLBody(err, result, response,
                        log);
                });
        } else if (request.query.uploadId !== undefined) {
            // POST complete multipart upload
            api.callApiMethod('completeMultipartUpload', request, log,
                (err, result) => {
                    _pushMetrics(err, utapi, 'completeMultipartUpload',
                        request.bucketName);
                    return routesUtils.responseXMLBody(err, result, response,
                        log);
                });
        } else {
            routesUtils.responseNoBody(errors.InternalError, null, response,
                200, log);
        }
    });
}
