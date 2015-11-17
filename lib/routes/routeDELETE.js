import utils from '../utils';
import { checkAuth } from '../auth/checkAuth';
import bucketDelete from '../api/bucketDelete';
import objectDelete from '../api/objectDelete';
import multipartDelete from '../api/multipartDelete';

export default function routeDELETE(request, response, datastore, metastore) {
    utils.normalizeRequest(request);
    checkAuth(request, function checkAuthRes(err, accessKey) {
        if (err) {
            return utils.errorXmlResponse(response, err);
        }

        if (utils.getResourceNames(request).object === undefined) {
            // delete bucket
            bucketDelete(accessKey, metastore, request, (err) => {
                if (err) {
                    return utils.errorXmlResponse(response, err);
                }
                utils.buildResponseHeaders(response, {});
                return utils.okHeaderResponse(response, 204);
            });
        } else {
            if (request.query.uploadId) {
                multipartDelete(accessKey, datastore, metastore, request,
                    (err) => {
                        if (err) {
                            return utils
                                .errorXmlResponse(response, err);
                        }
                        return utils.okHeaderResponse(response, 204);
                    });
            } else {
                objectDelete(accessKey, datastore, metastore, request,
                    (err, result, responseHeaders) => {
                        if (err) {
                            return utils.errorXmlResponse(response, err);
                        }
                        utils.buildResponseHeaders(response, responseHeaders);
                        return utils.okHeaderResponse(response, 204);
                    });
            }
        }
    });
}
