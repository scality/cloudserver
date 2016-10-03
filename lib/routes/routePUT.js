import { errors } from 'arsenal';
import { parseString } from 'xml2js';

import api from '../api/api';
import routesUtils from './routesUtils';
import utils from '../utils';
import pushMetrics from '../utilities/pushMetrics';

export default function routePUT(request, response, log, utapi) {
    log.debug('routing request', { method: 'routePUT' });

    if (request.objectKey === undefined || request.objectKey === '/') {
        request.post = '';
        request.on('data', chunk => request.post += chunk.toString());

        request.on('end', () => {
            // PUT bucket ACL
            if (request.query.acl !== undefined) {
                api.callApiMethod('bucketPutACL', request, log, err => {
                    pushMetrics(err, log, utapi, 'bucketPutACL',
                        request.bucketName);
                    return routesUtils.responseNoBody(err, null, response, 200,
                        log);
                });
            } else if (request.query.acl === undefined) {
                // PUT bucket
                if (request.post) {
                    const xmlToParse = request.post;
                    return parseString(xmlToParse, (err, result) => {
                        if (err || !result.CreateBucketConfiguration
                            || !result.CreateBucketConfiguration
                                .LocationConstraint
                            || !result.CreateBucketConfiguration
                                .LocationConstraint[0]) {
                            log.debug('request xml is malformed');
                            return routesUtils.responseNoBody(errors
                                .MalformedXML,
                                null, response, null, log);
                        }
                        const locationConstraint =
                            result.CreateBucketConfiguration
                            .LocationConstraint[0];
                        log.trace('location constraint',
                            { locationConstraint });
                        return api.callApiMethod('bucketPut', request, log,
                        err => {
                            pushMetrics(err, log, utapi, 'bucketPut',
                                request.bucketName);
                            return routesUtils.responseNoBody(err, null,
                              response, 200, log);
                        }, locationConstraint);
                    });
                }
                return api.callApiMethod('bucketPut', request, log, err => {
                    pushMetrics(err, log, utapi, 'bucketPut',
                        request.bucketName);
                    return routesUtils.responseNoBody(err, null, response, 200,
                        log);
                });
            }
        });
    } else {
        // PUT object, PUT object ACL, PUT object multipart or
        // PUT object copy
        // if content-md5 is not present in the headers, try to
        // parse content-md5 from meta headers
        if (request.headers['content-md5']) {
            request.contentMD5 = request.headers['content-md5'];
        } else {
            request.contentMD5 = utils.parseContentMD5(request.headers);
        }
        if (request.contentMD5 && request.contentMD5.length !== 32) {
            request.contentMD5 = new Buffer(request.contentMD5, 'base64')
                .toString('hex');
            if (request.contentMD5 && request.contentMD5.length !== 32) {
                log.warn('invalid md5 digest', {
                    contentMD5: request.contentMD5,
                });
                return routesUtils
                    .responseNoBody(errors.InvalidDigest, null, response, 200,
                                    log);
            }
        }
        if (request.query.partNumber) {
            if (request.headers['x-amz-copy-source']) {
                api.callApiMethod('objectPutCopyPart', request, log,
                (err, xml,
                    additionalHeaders) =>
                    routesUtils.responseXMLBody(err, xml, response, log,
                        additionalHeaders)
                );
            } else {
                api.callApiMethod('objectPutPart', request, log,
                    (err, calculatedHash) => {
                        // ETag's hex should always be enclosed in quotes
                        const resMetaHeaders = { ETag: `"${calculatedHash}"` };
                        pushMetrics(err, log, utapi, 'objectPutPart',
                            request.bucketName, request.parsedContentLength);
                        routesUtils.responseNoBody(err, resMetaHeaders,
                            response, 200, log);
                    });
            }
        } else if (request.query.acl !== undefined) {
            request.post = '';
            request.on('data', chunk => request.post += chunk.toString());
            request.on('end', () => {
                api.callApiMethod('objectPutACL', request, log, err => {
                    pushMetrics(err, log, utapi, 'objectPutACL',
                        request.bucketName);
                    return routesUtils.responseNoBody(err, null, response, 200,
                        log);
                });
            });
        } else if (request.headers['x-amz-copy-source']) {
            return api.callApiMethod('objectCopy', request, log, (err, xml,
                additionalHeaders, sourceObjSize, destObjPrevSize) => {
                pushMetrics(err, log, utapi, 'objectCopy', request.bucketName,
                    sourceObjSize, destObjPrevSize);
                routesUtils.responseXMLBody(err, xml, response, log,
                    additionalHeaders);
            });
        } else {
            if (Number.isNaN(request.parsedContentLength)) {
                return routesUtils.responseNoBody(errors.MissingContentLength,
                    null, response, 411, log);
            }
            if (Number.isNaN(request.parsedContentLength)) {
                return routesUtils.responseNoBody(errors.InvalidArgument,
                    null, response, 400, log);
            }
            log.end().addDefaultFields({
                contentLength: request.parsedContentLength,
            });
            api.callApiMethod('objectPut', request, log,
                (err, contentMD5, prevContentLen) => {
                    // ETag's hex should always be enclosed in quotes
                    const resMetaHeaders = {
                        ETag: `"${contentMD5}"`,
                    };
                    pushMetrics(err, log, utapi, 'objectPut',
                        request.bucketName, request.parsedContentLength,
                        prevContentLen);
                    return routesUtils.responseNoBody(err, resMetaHeaders,
                        response, 200, log);
                });
        }
    }
}
