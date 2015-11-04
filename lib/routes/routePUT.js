import crypto from 'crypto';

import utils from '../utils';
import { checkAuth } from '../auth/checkAuth';
import bucketPut from '../api/bucketPut';
import objectPut from '../api/objectPut';
import bucketPutACL from '../api/bucketPutACL';

export default function routePUT(request, response, datastore, metastore) {
    utils.normalizeRequest(request);
    checkAuth(request, function checkAuthRes(err, accessKey) {
        if (err) {
            return utils.errorXmlResponse(response, err);
        }
        const objectKey = utils.getResourceNames(request).object;
        if (objectKey === undefined || objectKey === '/') {
            if (request.query.acl !== undefined) {
                console.log("setting bucket acl");
                bucketPutACL(accessKey, metastore, request, (err) => {
                    if (err) {
                        console.log("err from bucket acl", err);
                        return utils.errorXmlResponse(response, err);
                    }
                    return utils.okHeaderResponse(response, 200);
                });
            } else if (request.query.acl === undefined) {
                bucketPut(accessKey, metastore, request, (err) => {
                    if (err) {
                        return utils.errorXmlResponse(response, err);
                    }
                    return utils.okHeaderResponse(response, 200);
                });
            }
        } else {
            if (request.headers.expect === '100-continue') {
                response.writeHead(100);
            }
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
                if (request.lowerCaseHeaders['content-length'] === undefined) {
                    request.lowerCaseHeaders['content-length'] = contentLength;
                }
                request.calculatedMD5 = md5Hash.digest('hex');
                objectPut(accessKey, datastore, metastore, request, (err) => {
                    if (err) {
                        return utils.errorXmlResponse(response, err);
                    }
                    return utils.okHeaderResponse(response, 200);
                });
            });
        }
    });
}
