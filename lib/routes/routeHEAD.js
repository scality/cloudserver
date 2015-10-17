const utils = require('../utils.js');
const checkAuth = require("../auth/checkAuth.js").checkAuth;

const bucketHead = require('../api/bucketHead.js');
const objectHead = require('../api/objectHead.js');

const routeHEAD = function routeHEAD(request, response, datastore, metastore) {
    utils.normalizeRequest(request);
    checkAuth(request, function checkAuthRes(err, accessKey) {
        if (err) {
            return utils.errorResponse(response, 'Access Denied', 403);
        }
        const resourceRes = utils.getResourceNames(request).object;
        const objectKey = resourceRes.object;

        if (objectKey === undefined) {
            bucketHead(
                accessKey,
                metastore,
                request,
                function bucketHeadRes(err) {
                    if (err) {
                        return utils.errorResponse(response, err);
                    }
                    return utils.okHeaderResponse(response, 200);
                }
            );
        } else {
            objectHead(
                accessKey,
                metastore,
                request,
                function objectHeadRes(err, responseMetaHeaders) {
                    if (err) {
                        return utils.errorResponse(response, err, 404);
                    }
                    utils.buildSuccessResponse(
                        request.lowerCaseHeaders,
                        response,
                        responseMetaHeaders
                    );
                    return response.end();
                }
            );
        }

        return utils.errorResponse(response, "Invalid request");
    });
};

module.exports = routeHEAD;
