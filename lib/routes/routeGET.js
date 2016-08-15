import api from '../api/api';
import routesUtils from './routesUtils';

function _pushMetrics(err, utapi, action, resource, contentLength) {
    if (!err) {
        const timestamp = Date.now();
        if (action === 'bucketGetACL') {
            utapi.pushMetricGetBucketAcl(resource, timestamp);
        } else if (action === 'listMultipartUploads') {
            utapi.pushMetricListBucketMultipartUploads(resource, timestamp);
        } else if (action === 'bucketGet') {
            utapi.pushMetricListBucket(resource, timestamp);
        } else if (action === 'objectGetACL') {
            utapi.pushMetricGetObjectAcl(resource, timestamp);
        } else if (action === 'listParts') {
            utapi.pushMetricListBucketMultipartUploads(resource, timestamp);
        } else if (action === 'objectGet') {
            utapi.pushMetricGetObject(resource, timestamp, contentLength);
        }
    }
}

export default function routerGET(request, response, log, utapi) {
    log.debug('routing request', { method: 'routerGET' });

    if (request.bucketName === undefined && request.objectKey === undefined) {
        // GET service
        api.callApiMethod('serviceGet', request, log, (err, xml) =>
            routesUtils.responseXMLBody(err, xml, response, log));
    } else if (request.objectKey === undefined) {
        // GET bucket ACL
        if (request.query.acl !== undefined) {
            api.callApiMethod('bucketGetACL', request, log, (err, xml) => {
                _pushMetrics(err, utapi, 'bucketGetACL', request.bucketName);
                return routesUtils.responseXMLBody(err, xml, response, log);
            });
        } else if (request.query.uploads !== undefined) {
            // List MultipartUploads
            api.callApiMethod('listMultipartUploads', request, log,
                (err, xml) => {
                    _pushMetrics(err, utapi, 'listMultipartUploads',
                        request.bucketName);
                    return routesUtils.responseXMLBody(err, xml, response, log);
                });
        } else {
            // GET bucket
            api.callApiMethod('bucketGet', request, log, (err, xml) => {
                _pushMetrics(err, utapi, 'bucketGet', request.bucketName);
                return routesUtils.responseXMLBody(err, xml, response, log);
            });
        }
    } else {
        if (request.query.acl !== undefined) {
            // GET object ACL
            api.callApiMethod('objectGetACL', request, log, (err, xml) => {
                _pushMetrics(err, utapi, 'objectGetACL', request.bucketName);
                return routesUtils.responseXMLBody(err, xml, response, log);
            });
            // List parts of an open multipart upload
        } else if (request.query.uploadId !== undefined) {
            api.callApiMethod('listParts', request, log, (err, xml) => {
                _pushMetrics(err, utapi, 'listParts', request.bucketName);
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
                _pushMetrics(err, utapi, 'objectGet', request.bucketName,
                    contentLength);
                return routesUtils.responseStreamData(err, request.headers,
                    resMetaHeaders, dataGetInfo, response, range, log);
            });
        }
    }
}
