import utils from '../utils';
import api from '../api/api';
import routesUtils from './routesUtils';

export default function routePOST(request, response) {
    utils.normalizeRequest(request);
    if (utils.getResourceNames(request).object === undefined) {
        return routesUtils.responseNoBody('InvalidURI');
    } else if (request.query.uploads !== undefined) {
        // POST multipart upload
        api.callApiMethod('initiateMultipartUpload', request, (err, result) =>
            routesUtils.responseXMLBody(err, result, response));
    } else if (request.query.uploadId !== undefined) {
        // POST complete multipart upload
        api.callApiMethod('completeMultipartUpload', request, (err, result) =>
            routesUtils.responseXMLBody(err, result, response));
    }
}
