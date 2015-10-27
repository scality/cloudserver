const utils = require('../utils.js');
const checkAuth = require("../auth/checkAuth.js").checkAuth;

const bucketDelete = require('../api/bucketDelete.js');
const objectDelete = require('../api/objectDelete.js');

const routeDELETE = function routeDELETE(request, response, datastore,
    metastore) {
    utils.normalizeRequest(request);
    checkAuth(request, function checkAuthRes(err, accessKey) {
        if (err) {
            return utils.errorXmlResponse(response, err);
        }

        const resourceRes = utils.getResourceNames(request);
        const objectKey = resourceRes.object;

        if (objectKey === undefined) {
            // delete bucket
            bucketDelete(
                accessKey,
                metastore,
                request,
                function bucketDeleteRes(err) {
                    if (err) {
                        return utils.errorXmlResponse(response, err);
                    }
                    utils.buildResponseHeaders(response, {});
                    return utils.okHeaderResponse(response, 204);
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
                        return utils.errorXmlResponse(response, err);
                    }
                    utils.buildResponseHeaders(response, responseHeaders);
                    return utils.okHeaderResponse(response, 204);
                }
            );
        }
    });
};

module.exports = routeDELETE;
