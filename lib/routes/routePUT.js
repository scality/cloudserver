import crypto from 'crypto';
import utils from '../utils';
import { checkAuth } from '../auth/checkAuth';
import bucketPut from '../api/bucketPut';
import objectPut from '../api/objectPut';
import bucketPutACL from '../api/bucketPutACL';
import objectPutACL from '../api/objectPutACL';
import objectPutPart from '../api/objectPutPart';

export default function routePUT(request, response, datastore, metastore) {
    utils.normalizeRequest(request);
    checkAuth(request, function checkAuthRes(err, accessKey) {
        if (err) {
            return utils.errorXmlResponse(response, err);
        }
        const objectKey = utils.getResourceNames(request).object;
        // bucket requests
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
            // object requests
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
                    if (!request.lowerCaseHeaders['content-md5']) {
                        const contentMD5Header =
                            utils.parseContentMD5(request.headers);
                        if (contentMD5Header) {
                            request.lowerCaseHeaders['content-md5'] =
                                contentMD5Header;
                        }
                    }

                    if (request.lowerCaseHeaders['content-md5']) {
                        if (request.lowerCaseHeaders['content-md5']
                            .length === 32) {
                            request.calculatedMD5 = md5Hash.digest('hex');
                        } else {
                            request.calculatedMD5 = md5Hash.digest('base64');
                        }
                    } else {
                        request.calculatedMD5 = md5Hash.digest('hex');
                    }
                    if (request.query.partNumber) {
                        objectPutPart(accessKey, datastore, metastore, request,
                            (err) => {
                                if (err) {
                                    return utils
                                        .errorXmlResponse(response, err);
                                }
                                response
                                    .setHeader('ETag', request.calculatedMD5);
                                return utils.okHeaderResponse(response, 200);
                            });
                    } else {
                        objectPut(accessKey, datastore, metastore, request,
                            (err) => {
                                if (err) {
                                    return utils
                                        .errorXmlResponse(response, err);
                                }
                                response
                                    .setHeader('ETag', request.calculatedMD5);
                                return utils.okHeaderResponse(response, 200);
                            });
                    }
                });
            } else {
                console.log("setting object acl");
                objectPutACL(accessKey, metastore, request, (err) => {
                    if (err) {
                        console.log("err from object acl", err);
                        return utils.errorXmlResponse(response, err);
                    }
                    return utils.okHeaderResponse(response, 200);
                });
            }
        }
    });
}
