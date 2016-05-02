import api from '../api/api';
import routesUtils from './routesUtils';
import { errors } from 'arsenal';

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
        } else if (request.query.policy !== undefined) {
            // Results in s3cmd returning 'none' for bucket policy on an info
            // request. TODO update when policy is implemented
            return routesUtils.responseXMLBody(errors.NotImplemented, null,
              response, log);
        } else if (request.query.policy !== undefined) {
            // Results in s3cmd returning 'none' for bucket policy on an info
            // request. TODO update when policy is implemented
            return routesUtils.responseXMLBody(errors.NotImplemented, null,
              response, log);
        } else if (request.query.cors !== undefined) {
            // Results in s3cmd returning 'none' for bucket cors on an info
            // request. TODO update when cors is implemented
            return routesUtils.responseXMLBody(errors.NotImplemented, null,
              response, log);
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
                    resMetaHeaders) => {
                routesUtils.responseStreamData(err, request.headers,
                    resMetaHeaders, dataGetInfo, response, log);
            });
        }
    }
}
