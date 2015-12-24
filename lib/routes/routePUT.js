import crypto from 'crypto';
import utils from '../utils';
import api from '../api/api';
import routesUtils from './routesUtils';

export default function routePUT(request, response) {
    utils.normalizeRequest(request);
    const objectKey = utils.getResourceNames(request).object;

    if (objectKey === undefined || objectKey === '/') {
        // PUT bucket ACL
        if (request.query.acl !== undefined) {
            api.callApiMethod('bucketPutACL', request, (err) =>
                routesUtils.responseNoBody(err, null, response));
        } else if (request.query.acl === undefined) {
            // PUT bucket
            api.callApiMethod('bucketPut', request, (err) =>
                routesUtils.responseNoBody(err, null, response));
        }
    } else {
        // PUT object, PUT object ACL or PUT object multipart
        const md5Hash = crypto.createHash('md5');
        const chunks = [];
        let contentLength = 0;

        request.on('data', function chunkReceived(chunk) {
            const cBuffer = new Buffer(chunk, "binary");
            contentLength += chunk.length;
            chunks.push(cBuffer);
            md5Hash.update(cBuffer);
        });

        request.on('end', function combineChunks() {
            if (chunks.length > 0) {
                request.post = Buffer.concat(chunks);
            }
            if (request.lowerCaseHeaders['content-length'] === undefined) {
                request.lowerCaseHeaders['content-length'] = contentLength;
            }
            // if content-md5 is not present in the headers, try to
            // parse content-md5 from meta headers
            let contentMD5Header;
            if (request.lowerCaseHeaders['content-md5']) {
                contentMD5Header = request.lowerCaseHeaders['content-md5'];
            } else {
                contentMD5Header = utils.parseContentMD5(request.headers);
            }

            request.calculatedMD5 = md5Hash.digest('hex');
            // some clients send base64, convert to hex
            // 32 chars = 16 bytes(2 chars-per-byte) = 128 bits of MD5 hex
            if (contentMD5Header && contentMD5Header.length !== 32) {
                contentMD5Header = new Buffer(contentMD5Header, 'base64')
                    .toString('hex');
                if (request.calculatedMD5 !== contentMD5Header) {
                    return routesUtils
                        .responseNoBody('InvalidDigest', null, response);
                }
            }

            if (request.query.partNumber) {
                api.callApiMethod('objectPutPart', request, (err) => {
                    // ETag's hex should always be enclosed in quotes
                    const resMetaHeaders = {
                        ETag: `"${request.calculatedMD5}"`
                    };
                    routesUtils.responseNoBody(err, resMetaHeaders, response);
                });
            } else if (request.query.acl !== undefined) {
                api.callApiMethod('objectPutACL', request, (err) =>
                    routesUtils.responseNoBody(err, null, response));
            } else {
                api.callApiMethod('objectPut', request, (err) => {
                    // ETag's hex should always be enclosed in quotes
                    const resMetaHeaders = {
                        ETag: `"${request.calculatedMD5}"`
                    };
                    routesUtils.responseNoBody(err, resMetaHeaders, response);
                });
            }
        });
    }
}
