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
        // PUT object or PUT object multipart
        if (request.query.acl === undefined) {
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
                    request.post = chunks;
                }
                if (request.lowerCaseHeaders['content-length'] ===
                    undefined) {
                    request.lowerCaseHeaders['content-length'] =
                        contentLength;
                }
                // if content-md5 is not present in the headers, try to
                // parse content-md5 from meta headers
                let contentMD5Header = '';
                if (request.lowerCaseHeaders['content-md5']) {
                    contentMD5Header = request.lowerCaseHeaders['content-md5'];
                } else {
                    contentMD5Header = utils.parseContentMD5(request.headers);
                }

                if (contentMD5Header) {
                    if (contentMD5Header.length === 32) {
                        request.calculatedMD5 = md5Hash.digest('hex');
                    } else {
                        request.calculatedMD5 = md5Hash.digest('base64');
                    }
                    if (request.calculatedMD5 !== contentMD5Header) {
                        return utils
                            .errorXMLResponse(response, 'InvalidDigest');
                    }
                } else {
                    request.calculatedMD5 = md5Hash.digest('hex');
                }

                if (request.query.partNumber) {
                    api.callApiMethod('objectPutPart', request, (err) => {
                        const resMetaHeaders = {
                            ETag: request.calculatedMD5
                        };
                        routesUtils.responseNoBody(err, resMetaHeaders,
                            response);
                    });
                } else {
                    api.callApiMethod('objectPut', request, (err) => {
                        const resMetaHeaders = {
                            ETag: request.calculatedMD5
                        };
                        routesUtils.responseNoBody(err, resMetaHeaders,
                            response);
                    });
                }
            });
        } else {
            api.callApiMethod('objectPutACL', request, (err) =>
                routesUtils.responseNoBody(err, null, response));
        }
    }
}
