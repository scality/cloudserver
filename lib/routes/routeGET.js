import api from '../api/api';
import { errors } from 'arsenal';
import routesUtils from './routesUtils';
import statsReport500 from '../utilities/statsReport500';

export default function routerGET(request, response, log, statsClient) {
    log.debug('routing request', { method: 'routerGET' });
    if (request.bucketName === undefined && request.objectKey !== undefined) {
        routesUtils.responseXMLBody(errors.NoSuchBucket, null, response, log);
    } else if (request.bucketName === undefined
        && request.objectKey === undefined) {
        // GET service
        api.callApiMethod('serviceGet', request, response, log, (err, xml) => {
            statsReport500(err, statsClient);
            return routesUtils.responseXMLBody(err, xml, response, log);
        });
    } else if (request.objectKey === undefined) {
        // GET bucket ACL
        if (request.query.acl !== undefined) {
            api.callApiMethod('bucketGetACL', request, response, log,
            (err, xml, corsHeaders) => {
                statsReport500(err, statsClient);
                return routesUtils.responseXMLBody(err, xml, response, log,
                    corsHeaders);
            });
        } else if (request.query.cors !== undefined) {
            api.callApiMethod('bucketGetCors', request, response, log,
                (err, xml, corsHeaders) => {
                    statsReport500(err, statsClient);
                    routesUtils.responseXMLBody(err, xml, response, log,
                        corsHeaders);
                });
        } else if (request.query.versioning !== undefined) {
            api.callApiMethod('bucketGetVersioning', request, response, log,
                (err, xml, corsHeaders) => {
                    statsReport500(err, statsClient);
                    routesUtils.responseXMLBody(err, xml, response, log,
                        corsHeaders);
                });
        } else if (request.query.website !== undefined) {
            api.callApiMethod('bucketGetWebsite', request, response, log,
                (err, xml, corsHeaders) => {
                    statsReport500(err, statsClient);
                    routesUtils.responseXMLBody(err, xml, response, log,
                        corsHeaders);
                });
        } else if (request.query.uploads !== undefined) {
            // List MultipartUploads
            api.callApiMethod('listMultipartUploads', request, response, log,
                (err, xml, corsHeaders) => {
                    statsReport500(err, statsClient);
                    return routesUtils.responseXMLBody(err, xml, response, log,
                        corsHeaders);
                });
        } else if (request.query.location !== undefined) {
            api.callApiMethod('bucketGetLocation', request, response, log,
                (err, xml, corsHeaders) => {
                    statsReport500(err, statsClient);
                    return routesUtils.responseXMLBody(err, xml, response, log,
                      corsHeaders);
                });
        } else {
            // GET bucket
            api.callApiMethod('bucketGet', request, response, log,
                (err, xml, corsHeaders) => {
                    statsReport500(err, statsClient);
                    return routesUtils.responseXMLBody(err, xml, response, log,
                        corsHeaders);
                });
        }
    } else {
        if (request.query.acl !== undefined) {
            // GET object ACL
            api.callApiMethod('objectGetACL', request, response, log,
                (err, xml, corsHeaders) => {
                    statsReport500(err, statsClient);
                    return routesUtils.responseXMLBody(err, xml, response, log,
                        corsHeaders);
                });
        } else if (request.query.tagging !== undefined) {
            // GET object Tagging
            api.callApiMethod('objectGetTagging', request, response, log,
                (err, xml, corsHeaders) => {
                    statsReport500(err, statsClient);
                    return routesUtils.responseXMLBody(err, xml, response, log,
                        corsHeaders);
                });
            // List parts of an open multipart upload
        } else if (request.query.uploadId !== undefined) {
            api.callApiMethod('listParts', request, response, log,
                (err, xml, corsHeaders) => {
                    statsReport500(err, statsClient);
                    return routesUtils.responseXMLBody(err, xml, response, log,
                        corsHeaders);
                });
        } else {
            // GET object
            api.callApiMethod('objectGet', request, response, log,
                (err, dataGetInfo, resMetaHeaders, range) => {
                    let contentLength = 0;
                    if (resMetaHeaders && resMetaHeaders['Content-Length']) {
                        contentLength = resMetaHeaders['Content-Length'];
                    }
                    log.end().addDefaultFields({ contentLength });
                    statsReport500(err, statsClient);
                    return routesUtils.responseStreamData(err, request.headers,
                        resMetaHeaders, dataGetInfo, response, range, log);
                });
        }
    }
}
