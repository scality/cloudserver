const crypto = require('crypto');

const utils = require('../utils.js');
const checkAuth = require("../auth/checkAuth.js").checkAuth;

const bucketPut = require('../api/bucketPut.js');
const objectPut = require('../api/objectPut.js');

const routePUT = function routePUT(request, response, datastore, metastore) {
    utils.normalizeRequest(request);

    checkAuth(request, function checkAuthRes(err, accessKey) {
        if (err) {
            return utils.errorResponse(response, 'AccessDenied', 403);
        }

        const resourceRes = utils.getResourceNames(request);
        const objectKey = resourceRes.object;
        if (objectKey === undefined || objectKey === '/') {
            bucketPut(
                accessKey,
                metastore,
                request,
                function bucketPutRes(err) {
                    if (err) {
                        return utils.errorResponse(response, err, 500);
                    }
                    return utils.okHeaderResponse(response, 200);
                }
            );
        } else {
            if (request.headers.expect === '100-continue') {
                response.writeHead(100);
            }

            const md5Hash = crypto.createHash('md5');
            const chunks = [];
            request.on('data', function chunkReceived(chunk) {
                const cBuffer = new Buffer(chunk, "binary");
                chunks.push(cBuffer);
                md5Hash.update(cBuffer);
            });

            request.on('end', function combineChunks() {
                if (chunks.length > 0) {
                    request.post = chunks;
                }

                request.calculatedMD5 = md5Hash.digest('hex');
                objectPut(
                    accessKey,
                    datastore,
                    metastore,
                    request,
                    function objectPutRes(err) {
                        if (err) {
                            return utils.errorResponse(response, err);
                        }
                        return utils.okHeaderResponse(response, 200);
                    }
                );
            });
        }
    });
};

module.exports = routePUT;
