import api from '../api/api';
import { errors } from 'arsenal';
import routesUtils from './routesUtils';
import pushMetrics from '../utilities/pushMetrics';

export default function routerGET(request, response, log, utapi) {
    log.debug('routing request', { method: 'routerGET' });
    if (request.bucketName === undefined && request.objectKey !== undefined) {
        routesUtils.responseXMLBody(errors.NoSuchBucket, null, response, log);
    } else if (request.bucketName === undefined
        && request.objectKey === undefined) {
        // GET service
        api.callApiMethod('serviceGet', request, log, (err, xml) =>
            routesUtils.responseXMLBody(err, xml, response, log));
    } else if (request.objectKey === undefined) {
        // GET bucket ACL
        if (request.query.acl !== undefined) {
            api.callApiMethod('bucketGetACL', request, log, (err, xml) => {
                pushMetrics(err, log, utapi, 'bucketGetACL',
                    request.bucketName);
                return routesUtils.responseXMLBody(err, xml, response, log);
            });
        } else if (request.query.versioning !== undefined) {
            api.callApiMethod('bucketGetVersioning', request, log,
                (err, xml) => {
                    // TODO push metrics for gucketGetVersioning
                    // pushMetrics(err, log, utapi, 'bucketGetVersioning',
                    //     request.bucketName);
                    routesUtils.responseXMLBody(err, xml, response, log);
                });
        } else if (request.query.versions !== undefined) {
            // GET bucket
            api.callApiMethod('bucketGetVersions', request, log, (err, xml) => {
                // use the same metrics as bucketGet
                pushMetrics(err, log, utapi, 'bucketGet', request.bucketName);
                return routesUtils.responseXMLBody(err, xml, response, log);
            });
        } else if (request.query.uploads !== undefined) {
            // List MultipartUploads
            api.callApiMethod('listMultipartUploads', request, log,
                (err, xml) => {
                    pushMetrics(err, log, utapi, 'listMultipartUploads',
                        request.bucketName);
                    return routesUtils.responseXMLBody(err, xml, response, log);
                });
        } else {
            // GET bucket
            api.callApiMethod('bucketGet', request, log, (err, xml) => {
                pushMetrics(err, log, utapi, 'bucketGet', request.bucketName);
                return routesUtils.responseXMLBody(err, xml, response, log);
            });
        }
    } else {
        if (request.query.acl !== undefined) {
            // GET object ACL
            api.callApiMethod('objectGetACL', request, log, (err, xml) => {
                pushMetrics(err, log, utapi, 'objectGetACL',
                    request.bucketName);
                return routesUtils.responseXMLBody(err, xml, response, log);
            });
            // List parts of an open multipart upload
        } else if (request.query.uploadId !== undefined) {
            api.callApiMethod('listParts', request, log, (err, xml) => {
                pushMetrics(err, log, utapi, 'listParts', request.bucketName);
                return routesUtils.responseXMLBody(err, xml, response, log);
            });
        } else {
            // GET object
            api.callApiMethod('objectGet', request, log, (err, dataGetInfo,
                    resMetaHeaders, range) => {
                let contentLength = 0;
                if (resMetaHeaders && resMetaHeaders['Content-Length']) {
                    contentLength = resMetaHeaders['Content-Length'];
                }
                log.end().addDefaultFields({ contentLength });
                pushMetrics(err, log, utapi, 'objectGet', request.bucketName,
                    contentLength);
                return routesUtils.responseStreamData(err, request.headers,
                    resMetaHeaders, dataGetInfo, response, range, log);
            });
        }
    }
}
