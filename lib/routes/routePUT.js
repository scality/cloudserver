import api from '../api/api';
import routesUtils from './routesUtils';
import utils from '../utils';

export default function routePUT(request, response, log) {
    if (request.objectKey === undefined || request.objectKey === '/') {
        request.post = '';
        request.on('data', chunk => request.post += chunk.toString());

        request.on('end', () => {
            // PUT bucket ACL
            if (request.query.acl !== undefined) {
                log.info(`Received PUT Bucket ACL: ${request.url}`);
                api.callApiMethod('bucketPutACL', request, log, (err) =>
                    routesUtils.responseNoBody(err, null, response, 200, log));
            } else if (request.query.acl === undefined) {
                // PUT bucket
                log.info(`Received PUT Bucket: ${request.url}`);
                api.callApiMethod('bucketPut', request, log, (err) =>
                    routesUtils.responseNoBody(err, null, response, 200, log));
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
                log.error('Invalid MD5 Digest');
                return routesUtils
                .responseNoBody('InvalidDigest', null, response, log);
            }
        }
        if (request.query.partNumber) {
            log.info(`Received PUT Object part: ${request.url}`);
            api.callApiMethod('objectPutPart', request, log, (err) => {
                // ETag's hex should always be enclosed in quotes
                const resMetaHeaders = {
                    ETag: `"${request.calculatedMD5}"`
                };
                routesUtils.responseNoBody(err, resMetaHeaders, response,
                    200, log);
            });
        } else if (request.query.acl !== undefined) {
            log.info(`Received PUT Object ACL: ${request.url}`);
            request.post = '';
            request.on('data', chunk => request.post += chunk.toString());
            request.on('end', () => {
                api.callApiMethod('objectPutACL', request, log, (err) =>
                    routesUtils.responseNoBody(err, null, response, 200, log));
            });
        } else {
            log.info(`Received PUT Object: ${request.url}`);
            api.callApiMethod('objectPut', request, log, (err) => {
                // ETag's hex should always be enclosed in quotes
                const resMetaHeaders = {
                    ETag: `"${request.calculatedMD5}"`
                };
                routesUtils.responseNoBody(err, resMetaHeaders, response,
                    200, log);
            });
        }
    }
}
