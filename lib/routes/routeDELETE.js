import utils from '../utils';
import Auth from '../auth/checkAuth';
import bucketDelete from '../api/bucketDelete';
import objectDelete from '../api/objectDelete';
import multipartDelete from '../api/multipartDelete';

export default function routeDELETE(request, response, metastore) {
    utils.normalizeRequest(request);
    Auth.checkAuth(request, function checkAuthRes(err, accessKey) {
        if (err) {
            return utils.errorXmlResponse(response, err);
        }

        if (utils.getResourceNames(request).object === undefined) {
            bucketDelete(accessKey, metastore, request, (err) => {
                if (err) {
                    return utils.errorXmlResponse(response, err);
                }
                utils.buildResponseHeaders(response, {});
                return utils.okHeaderResponse(response, 204);
            });
        } else {
            if (request.query.uploadId) {
                multipartDelete(accessKey, metastore, request,
                    (err) => {
                        if (err) {
                            return utils
                                .errorXmlResponse(response, err);
                        }
                        return utils.okHeaderResponse(response, 204);
                    });
            } else {
                objectDelete(accessKey, metastore, request,
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
