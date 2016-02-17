import api from '../api/api';
import routesUtils from './routesUtils';
import utils from '../utils';

export default function routePUT(request, response, log) {
    log.info('received request', { method: 'routePUT' });

    if (request.objectKey === undefined || request.objectKey === '/') {
        request.post = '';
        request.on('data', chunk => request.post += chunk.toString());

        request.on('end', () => {
            // PUT bucket ACL
            if (request.query.acl !== undefined) {
                api.callApiMethod('bucketPutACL', request, log, (err) =>
                    routesUtils.responseNoBody(
                        err, null, response, 200, log,
                        routesUtils.onRequestEnd(request)
                ));
            } else if (request.query.acl === undefined) {
                // PUT bucket
                api.callApiMethod('bucketPut', request, log, (err) =>
                    routesUtils.responseNoBody(
                        err, null, response, 200, log,
                        routesUtils.onRequestEnd(request)
                ));
            }
        });
    } else {
        // PUT object, PUT object ACL or PUT object multipart
        // if content-md5 is not present in the headers, try to
        // parse content-md5 from meta headers
        if (request.lowerCaseHeaders['content-md5']) {
            request.contentMD5 = request.lowerCaseHeaders['content-md5'];
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
                    .responseNoBody('InvalidDigest', null, response, log,
                                    routesUtils.onRequestEnd(request));
            }
        }
        if (request.query.partNumber) {
            api.callApiMethod('objectPutPart', request, log, (err) => {
                // ETag's hex should always be enclosed in quotes
                const resMetaHeaders = {
                    ETag: `"${request.calculatedHash}"`
                };
                routesUtils.responseNoBody(err, resMetaHeaders, response,
                    200, log, routesUtils.onRequestEnd(request));
            });
        } else if (request.query.acl !== undefined) {
            request.post = '';
            request.on('data', chunk => request.post += chunk.toString());
            request.on('end', () => {
                api.callApiMethod('objectPutACL', request, log, (err) =>
                    routesUtils.responseNoBody(
                        err, null, response, 200, log,
                        routesUtils.onRequestEnd(request)
                ));
            });
        } else {
            api.callApiMethod('objectPut', request, log, (err) => {
                // ETag's hex should always be enclosed in quotes
                const resMetaHeaders = {
                    ETag: `"${request.calculatedHash}"`
                };
                routesUtils.responseNoBody(err, resMetaHeaders, response,
                    200, log, routesUtils.onRequestEnd(request));
            });
        }
    }
}
