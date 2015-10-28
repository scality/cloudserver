const Readable = require('stream').Readable;

const utils = require('../utils.js');
const checkAuth = require("../auth/checkAuth.js").checkAuth;

const serviceGet = require('../api/serviceGet.js');
const bucketGet = require('../api/bucketGet.js');
const objectGet = require('../api/objectGet.js');

const routerGET = function routerGET(request, response, datastore, metastore) {
    utils.normalizeRequest(request);
    checkAuth(request, function checkAuthRes(err, accessKey) {
        if (err) {
            return utils.errorXmlResponse(response, err);
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
                        return utils.errorXmlResponse(response, err);
                    }
                    return utils.okXmlResponse(response, xml);
                });
        } else if (objectKey === undefined) {
            // GET bucket
            bucketGet(
                accessKey,
                metastore,
                request,
                function bucketGetRes(err, xml) {
                    if (err) {
                        return utils.errorXmlResponse(response, err);
                    }
                    return utils.okXmlResponse(response, xml);
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
                        return utils.errorXmlResponse(response, err);
                    }

                    const readStream = new Readable;
                    let i;
                    for (i = 0; i < result.length; i += 1) {
                        readStream.push(result[i]);
                    }
                    // signal end of stream
                    readStream.push(null);

                    utils.buildSuccessResponse(
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
};

module.exports = routerGET;
