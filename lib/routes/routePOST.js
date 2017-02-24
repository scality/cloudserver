import { errors } from 'arsenal';

import api from '../api/api';
import routesUtils from './routesUtils';

/* eslint-disable no-param-reassign */
export default function routePOST(request, response, log) {
    log.debug('routing request', { method: 'routePOST' });

    const invalidMultiObjectDelReq = request.query.delete !== undefined
        && request.bucketName === undefined;
    if (invalidMultiObjectDelReq) {
        return routesUtils.responseNoBody(errors.MethodNotAllowed, null,
            response, null, log);
    }

    request.post = '';

    const invalidInitiateMpuReq = request.query.uploads !== undefined
        && request.objectKey === undefined;
    const invalidCompleteMpuReq = request.query.uploadId !== undefined
        && request.objectKey === undefined;
    if (invalidInitiateMpuReq || invalidCompleteMpuReq) {
        return routesUtils.responseNoBody(errors.InvalidURI, null,
            response, null, log);
    }

    // POST initiate multipart upload
    if (request.query.uploads !== undefined) {
        return api.callApiMethod('initiateMultipartUpload', request,
            response, log, (err, result, corsHeaders) =>
            routesUtils.responseXMLBody(err, result, response, log,
                corsHeaders));
    }

    // POST complete multipart upload
    if (request.query.uploadId !== undefined) {
        return api.callApiMethod('completeMultipartUpload', request,
            response, log, (err, result, corsHeaders) =>
            routesUtils.responseXMLBody(err, result, response, log,
                corsHeaders));
    }

    // POST multiObjectDelete
    if (request.query.delete !== undefined) {
        return api.callApiMethod('multiObjectDelete', request, response,
            log, (err, xml, corsHeaders) =>
            routesUtils.responseXMLBody(err, xml, response, log,
                corsHeaders));
    }

    return routesUtils.responseNoBody(errors.NotImplemented, null, response,
                200, log);
}
/* eslint-enable no-param-reassign */
