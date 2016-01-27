import api from '../api/api';
import routesUtils from './routesUtils';

export default function routerGET(request, response, log) {
    if (request.bucketName === undefined && request.objectKey === undefined) {
        // GET service
        log.info(`Received GET Service: ${request.url}`);
        api.callApiMethod('serviceGet', request, log, (err, xml) =>
            routesUtils.responseXMLBody(err, xml, response, log));
    } else if (request.objectKey === undefined) {
        // GET bucket ACL
        if (request.query.acl !== undefined) {
            log.info(`Received GET Bucket ACL: ${request.url}`);
            api.callApiMethod('bucketGetACL', request, log, (err, xml) =>
                routesUtils.responseXMLBody(err, xml, response, log));
        } else if (request.query.uploads !== undefined) {
            // List MultipartUploads
            log.info(`Received GET List MultiPartUploads: ${request.url}`);
            api.callApiMethod('listMultipartUploads', request, log,
                (err, xml) =>
                routesUtils.responseXMLBody(err, xml, response, log));
        } else {
            // GET bucket
            log.info(`Received GET Bucket: ${request.url}`);
            api.callApiMethod('bucketGet', request, log, (err, xml) =>
                routesUtils.responseXMLBody(err, xml, response, log));
        }
    } else {
        // GET object ACL
        if (request.query.acl !== undefined) {
            log.info(`Received GET Object ACL: ${request.url}`);
            api.callApiMethod('objectGetACL', request, log, (err, xml) =>
                routesUtils.responseXMLBody(err, xml, response, log));
            // List parts of an open multipart upload
        } else if (request.query.uploadId !== undefined) {
            log.info(`Received GET MultiPartUpload List parts: ${request.url}`);
            api.callApiMethod('listParts', request, log, (err, xml) =>
                routesUtils.responseXMLBody(err, xml, response, log));
        } else {
            // GET object
            log.info(`Received GET Object: ${request.url}`);
            api.callApiMethod('objectGet', request, log, (err, readStream,
                    resMetaHeaders) => {
                routesUtils.responseStreamData(err, request.lowerCaseHeaders,
                    resMetaHeaders, readStream, response, log);
            });
        }
    }
}
