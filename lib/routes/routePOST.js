import utils from '../utils';
import api from '../api/api';
import routesUtils from './routesUtils';

export default function routePOST(request, response, log) {
    utils.normalizeRequest(request);
    if (utils.getResourceNames(request).object === undefined) {
        log.info(`Received POST Invaild request: ${request.url}`);
        return routesUtils.responseNoBody('InvalidURI', null, response, 200,
            log);
    } else if (request.query.uploads !== undefined) {
        // POST multipart upload
        log.info(`Received POST Initiate Multipart Upload: ${request.url}`);
        api.callApiMethod('initiateMultipartUpload', request, log,
            (err, result) => routesUtils.responseXMLBody(err, result, response,
                log));
    } else if (request.query.uploadId !== undefined) {
        // POST complete multipart upload
        log.info(`Received POST Complete Multipart Upload: ${request.url}`);
        api.callApiMethod('completeMultipartUpload', request, log,
            (err, result) => routesUtils.responseXMLBody(err, result, response,
                log));
    } else {
        log.info(`Received Invaild POST request: ${request.url}`);
        routesUtils.responseNoBody('InternalError', null, response, 200,
            log);
    }
}
