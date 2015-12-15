import utils from '../utils';
import api from '../api/api';
import routesUtils from './routesUtils';

export default function routerGET(request, response) {
    utils.normalizeRequest(request);

    const resourceRes = utils.getResourceNames(request);
    const bucketname = resourceRes.bucket;
    const objectKey = resourceRes.object;

    if (bucketname === undefined && objectKey === undefined) {
        // GET service
        api.callApiMethod('serviceGet', request, (err, xml) =>
            routesUtils.responseXMLBody(err, xml, response));
    } else if (objectKey === undefined) {
        // GET bucket ACL
        if (request.query.acl !== undefined) {
            api.callApiMethod('bucketGetACL', request, (err, xml) =>
                routesUtils.responseXMLBody(err, xml, response));
        } else if (request.query.uploads !== undefined) {
            // List MultipartUploads
            api.callApiMethod('listMultipartUploads', request,  (err, xml) =>
                routesUtils.responseXMLBody(err, xml, response));
        } else {
            // GET bucket
            api.callApiMethod('bucketGet', request, (err, xml) =>
                routesUtils.responseXMLBody(err, xml, response));
        }
    } else {
        // GET object ACL
        if (request.query.acl !== undefined) {
            api.callApiMethod('objectGetACL', request, (err, xml) =>
                routesUtils.responseXMLBody(err, xml, response));
            // List parts of an open multipart upload
        } else if (request.query.uploadId !== undefined) {
            api.callApiMethod('listParts', request, (err, xml) =>
                routesUtils.responseXMLBody(err, xml, response));
        } else {
            // GET object
            api.callApiMethod('objectGet', request, (err, readStream,
                    resMetaHeaders) => {
                routesUtils.responseStreamData(err, request.lowerCaseHeaders,
                    resMetaHeaders, readStream, response);
            });
        }
    }
}
