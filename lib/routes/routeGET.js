import api from '../api/api';
import routesUtils from './routesUtils';

export default function routerGET(request, response, log) {
    log.info('received request', { method: 'routerGET' });

    if (request.bucketName === undefined && request.objectKey === undefined) {
        // GET service
        api.callApiMethod('serviceGet', request, log, (err, xml) =>
            routesUtils.responseXMLBody(err, xml, response, log));
    } else if (request.objectKey === undefined) {
        // GET bucket ACL
        if (request.query.acl !== undefined) {
            api.callApiMethod('bucketGetACL', request, log, (err, xml) =>
                routesUtils.responseXMLBody(err, xml, response, log));
        } else if (request.query.uploads !== undefined) {
            // List MultipartUploads
            api.callApiMethod('listMultipartUploads', request, log,
                (err, xml) =>
                routesUtils.responseXMLBody(err, xml, response, log));
        } else {
            // GET bucket
            api.callApiMethod('bucketGet', request, log, (err, xml) =>
                routesUtils.responseXMLBody(err, xml, response, log));
        }
    } else {
        if (request.query.acl !== undefined) {
            // GET object ACL
            api.callApiMethod('objectGetACL', request, log, (err, xml) =>
                routesUtils.responseXMLBody(err, xml, response, log));
            // List parts of an open multipart upload
        } else if (request.query.uploadId !== undefined) {
            api.callApiMethod('listParts', request, log, (err, xml) =>
                routesUtils.responseXMLBody(err, xml, response, log));
        } else {
            // GET object
            api.callApiMethod('objectGet', request, log, (err, dataGetInfo,
                    resMetaHeaders, range) => {
                if (resMetaHeaders && resMetaHeaders['Content-Length']) {
                    log.end().addDefaultFields({
                        contentLength: resMetaHeaders['Content-Length'],
                    });
                }
                routesUtils.responseStreamData(err, request.headers,
                    resMetaHeaders, dataGetInfo, response, range, log);
            });
        }
    }
}
