import { errors } from 'arsenal';

import api from '../api/api';
import routesUtils from './routesUtils';
import pushMetrics from '../utilities/pushMetrics';

/* eslint-disable no-param-reassign */
export default function routePOST(request, response, log, utapi) {
    log.debug('routing request', { method: 'routePOST' });

    const invalidMultiObjectDelReq = request.query.delete !== undefined
        && request.bucketName === undefined;
    if (invalidMultiObjectDelReq) {
        return routesUtils.responseNoBody(errors.MethodNotAllowed, null,
            response, null, log);
    }

    request.post = '';

    request.on('data', chunk => {
        request.post += chunk.toString();
    });

    request.on('end', () => {
        if (request.query.uploads !== undefined) {
            // POST multipart upload
            api.callApiMethod('initiateMultipartUpload', request, log,
                (err, result) => {
                    pushMetrics(err, log, utapi, 'initiateMultipartUpload',
                        request.bucketName);
                    return routesUtils.responseXMLBody(err, result, response,
                        log);
                });
        } else if (request.query.uploadId !== undefined) {
            // POST complete multipart upload
            api.callApiMethod('completeMultipartUpload', request, log,
                (err, result) => {
                    pushMetrics(err, log, utapi, 'completeMultipartUpload',
                        request.bucketName);
                    return routesUtils.responseXMLBody(err, result, response,
                        log);
                });
        } else if (request.query.delete !== undefined) {
            // POST multiObjectDelete
            api.callApiMethod('multiObjectDelete', request, log,
                (err, xml, totalDeletedContentLength, numOfObjects) => {
                    pushMetrics(err, log, utapi, 'multiObjectDelete',
                        request.bucketName, totalDeletedContentLength,
                        numOfObjects);
                    return routesUtils.responseXMLBody(err, xml, response,
                        log);
                });
        } else {
            routesUtils.responseNoBody(errors.NotImplemented, null, response,
                200, log);
        }
        return undefined;
    });
    return undefined;
}
/* eslint-enable no-param-reassign */
