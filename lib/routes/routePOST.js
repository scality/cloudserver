import { errors } from 'arsenal';

import api from '../api/api';
import routesUtils from './routesUtils';

/* eslint-disable no-param-reassign */
export default function routePOST(request, response, log) {
    log.debug('routing request', { method: 'routePOST' });
    request.post = '';

    request.on('data', chunk => {
        request.post += chunk.toString();
    });

    request.on('end', () => {
        if (request.query.uploads !== undefined) {
            // POST multipart upload
            api.callApiMethod('initiateMultipartUpload', request, log,
                (err, result) =>
                    routesUtils.responseXMLBody(err, result, response, log));
        } else if (request.query.uploadId !== undefined) {
            // POST complete multipart upload
            api.callApiMethod('completeMultipartUpload', request, log,
                (err, result) =>
                    routesUtils.responseXMLBody(err, result, response, log));
        } else if (request.query.delete !== undefined) {
            // POST multiObjectDelete
            api.callApiMethod('multiObjectDelete', request, log,
                (err, xml) =>
                    routesUtils.responseXMLBody(err, xml, response, log));
        } else {
            routesUtils.responseNoBody(errors.NotImplemented, null, response,
                200, log);
        }
        return undefined;
    });
}
/* eslint-enable no-param-reassign */
