module.exports = function services(router) {
    const utils = require('../utils.js');
    const checkAuth = require("../auth/checkAuth.js").checkAuth;
    const serviceGet = require('../api/serviceGet.js');
    const bucketHead = require('../api/bucketHead.js');
    const bucketGet = require('../api/bucketGet.js');
    const bucketPut = require('../api/bucketPut.js');
    const bucketDelete = require('../api/bucketDelete.js');
    const objectPut = require('../api/objectPut.js');
    const objectGet = require('../api/objectGet.js');
    const objectHead = require('../api/objectHead.js');
    const objectDelete = require('../api/objectDelete.js');
    const crypto = require('crypto');
    const Readable = require('stream').Readable;

    const datastore = {};
    const metastore = require('../testdata/metadata.json');

    const okHeaderResponse = function okHeaderResponse(response, code) {
        const httpCode = code || 500;
        response.writeHead(httpCode);
        return response.end();
    };

    const errorResponse = function errorResponse(response, msg, code) {
        const httpCode = code || 500;

        response.writeHead(httpCode, {
            'Content-type': 'text/javascript'
        });
        return response.end(JSON.stringify({
            error: msg
        }, null, 4));
    };

    const okXmlResponse = function okXmlResponse(response, xml) {
        response.writeHead(200, {
            'Content-type': 'application/xml'
        });
        return response.end(xml, 'utf8');
    };

    const errorXmlResponse = function errorXmlResponse(response, err) {
        const errorXmlRes = utils.buildResponseErrorXML(err);
        response.writeHead(errorXmlRes.httpCode, {
            'Content-type': 'application/xml'
        });
        return response.end(errorXmlRes.xml, 'utf8');
    };

    router.get("/(.*)", function routerGET(request, response) {
        utils.normalizeRequest(request);
        checkAuth(request, function checkAuthRes(err, accessKey) {
            if (err) {
                return errorResponse(response, 'Access Denied', 403);
            }

            const resourceRes = utils.getResourceNames(request);
            const bucketname = resourceRes.bucket;
            const objectKey = resourceRes.object;

            if (bucketname === undefined && objectKey === undefined) {
                // GET service
                serviceGet(
                    accessKey,
                    metastore,
                    request,
                    function serviceGetRes(err, xml) {
                        if (err) {
                            return errorResponse(response, err);
                        }
                        return okXmlResponse(response, xml);
                    });
            } else if (objectKey === undefined) {
                // GET bucket
                bucketGet(
                    accessKey,
                    metastore,
                    request,
                    function bucketGetRes(err, xml) {
                        if (err) {
                            return errorResponse(response, err);
                        }
                        return okXmlResponse(response, xml);
                    });
            } else {
                // GET object
                objectGet(
                    accessKey,
                    datastore,
                    metastore,
                    request,
                    function objectGetRes(err, result, responseMetaHeaders) {
                        if (err) {
                            return errorXmlResponse(response, err);
                        }

                        const readStream = new Readable;
                        let i;
                        for (i = 0; i < result.length; i += 1) {
                            readStream.push(result[i]);
                        }
                        // signal end of stream
                        readStream.push(null);

                        utils.buildGetSuccessfulResponse(
                            request.lowerCaseHeaders,
                            response,
                            responseMetaHeaders
                        );
                        readStream.pipe(response, {
                            end: false
                        });
                        readStream.on('end', function readStreamRes() {
                            response.end();
                        });
                    }
                );
            }
        });
    });


    /**
     * PUT resource - supports both bucket and object
     * If bucket name is in hostname then
     * the PUT is for creating the object in the bucket
     * or else the PUT is for creating a new bucket
     * @param {string} path style - It can be /<bucket name> or /<object name>
     * @param {function} callback - Callback to be called upon completion
     */
    router.put('/:resource', function routerPUT(request, response) {
        utils.normalizeRequest(request);

        checkAuth(request, function checkAuthRes(err, accessKey) {
            if (err) {
                return errorResponse(response, 'Access Denied', 403);
            }

            const bucketname = utils.getBucketNameFromHost(request);

            /* If bucket name is not in hostname, create a new bucket */
            if (bucketname === undefined) {
                bucketPut(
                    accessKey,
                    metastore,
                    request,
                    function bucketPutRes(err) {
                        if (err) {
                            return errorResponse(response, err, 500);
                        }
                        return okHeaderResponse(response, 200);
                    }
                );
            }

            if (request.headers.expect === '100-continue') {
                response.writeHead(100);
            }

            /* Create object if bucket name is in the hostname */
            if (bucketname) {
                objectPut(
                    accessKey,
                    datastore,
                    metastore,
                    request,
                    function objectPutRes(err) {
                        if (err) {
                            return errorResponse(response, err);
                        }
                        return okHeaderResponse(response, 200);
                    }
                );
            }
        });
    });

    // Put object in bucket where bucket is named in host or path
    router.putraw('/:resource/(.*)', function routerPUTRAW(request, response) {
        utils.normalizeRequest(request);
        checkAuth(request, function checkAuthRes(err, accessKey) {
            if (err) {
                return errorResponse(response, 'Access Denied', 403);
            }

            if (request.headers.expect === '100-continue') {
                response.writeHead(100);
            }

            const md5Hash = crypto.createHash('md5');
            // Put object using bucket name in path
            if (utils.getBucketNameFromHost(request) === undefined) {
                const chunks = [];

                request.on('data', function chunkReceived(chunk) {
                    const cBuffer = new Buffer(chunk, "binary");
                    chunks.push(cBuffer);
                    md5Hash.update(cBuffer);
                });

                request.on('end', function combineChunks() {
                    request.post = chunks;
                    request.calculatedMD5 = md5Hash.digest('hex');
                    objectPut(
                        accessKey,
                        datastore,
                        metastore,
                        request,
                        function objectPutRes(err) {
                            if (err) {
                                return errorResponse(response, err);
                            }
                            return okHeaderResponse(response, 200);
                        }
                    );
                });
            }

            // Put object using bucket name in host
            if (utils.getBucketNameFromHost(request)) {
                const chunks = [];

                request.on('data', function chunkReceived(chunk) {
                    const cBuffer = new Buffer(chunk, "binary");
                    chunks.push(cBuffer);
                    md5Hash.update(cBuffer);
                });

                request.on('end', function combineChunks() {
                    request.post = chunks;
                    request.calculatedMD5 = md5Hash.digest('hex');
                    objectPut(
                        accessKey,
                        datastore,
                        metastore,
                        request,
                        function objectPutRes(err) {
                            if (err) {
                                return errorResponse(response, err);
                            }
                            return okHeaderResponse(response, 200);
                        }
                    );
                });
            }
        });
    });

    /**
     * DELETE resource - deletes bucket or object
     * @param {string} path style - It can be /<bucket name> or /<object name>
     * @param {function} callback with request and response objects
     * @return {object} error or success response
     */

    router.delete('/(.*)', function routerDELETE(request, response) {
        utils.normalizeRequest(request);
        checkAuth(request, function checkAuthRes(err, accessKey) {
            if (err) {
                return errorResponse(response, 'Access Denied', 403);
            }

            const resourceRes = utils.getResourceNames(request).object;
            const objectKey = resourceRes.object;

            if (objectKey === undefined) {
                // delete bucket
                bucketDelete(
                    accessKey,
                    metastore,
                    request,
                    function bucketDeleteRes(err, result, resHeaders) {
                        if (err) {
                            return errorXmlResponse(response, err);
                        }
                        utils.buildResponseHeaders(
                            response,
                            resHeaders.headers
                        );
                        return okHeaderResponse(response, 204);
                    }
                );
            } else {
                // delete object
                objectDelete(
                    accessKey,
                    datastore,
                    metastore,
                    request,
                    function objectDeleteRes(err, result, responseHeaders) {
                        if (err) {
                            return errorResponse(response, err);
                        }
                        utils.buildResponseHeaders(response, responseHeaders);
                        return okHeaderResponse(response, 204);
                    }
                );
            }
        });
    });

    router.head("/", function routerHEAD(request, response) {
        utils.normalizeRequest(request);
        checkAuth(request, function checkAuthRes(err, accessKey) {
            if (err) {
                return errorResponse(response, 'Access Denied', 403);
            }

            // If bucket name in host, HEAD Bucket
            if (utils.getBucketNameFromHost(request) !== undefined) {
                bucketHead(
                    accessKey,
                    metastore,
                    request,
                    function bucketHeadRes(err) {
                        if (err) {
                            return errorResponse(response, err);
                        }
                        return okHeaderResponse(response, 200);
                    }
                );
            }

            // No route for "any" without a bucket name in host.
            return errorResponse(response, "Invalid request");
        });
    });

    router.head("/:resource", function routerHEAD(request, response) {
        utils.normalizeRequest(request);
        checkAuth(request, function checkAuthRes(err, accessKey) {
            if (err) {
                return errorResponse(response, 'Access Denied', 403);
            }

            // HEAD Bucket using bucket name in path
            if (utils.getBucketNameFromHost(request) === undefined) {
                bucketHead(
                    accessKey,
                    metastore,
                    request,
                    function bucketHeadRes(err) {
                        if (err) {
                            return errorResponse(response, err);
                        }
                        return okHeaderResponse(response, 200);
                    }
                );
            }
        });
    });


    router.head("/:resource/(.*)", function routerHEAD(request, response) {
        utils.normalizeRequest(request);
        checkAuth(request, function checkAuthRes(err, accessKey) {
            if (err) {
                return errorResponse(response, 'Access Denied', 403);
            }
            // HEAD Object using bucket name in path
            // or
            // HEAD Object using buckent name in host
            // (meaning object = :resource/:furtherObjectName)

            objectHead(
                accessKey,
                metastore,
                request,
                function objectHeadRes(err, responseMetaHeaders) {
                    if (err) {
                        return errorResponse(response, err, 404);
                    }
                    utils.buildGetSuccessfulResponse(
                        request.lowerCaseHeaders,
                        response,
                        responseMetaHeaders
                    );
                    return response.end();
                }
            );
        });
    });
};
