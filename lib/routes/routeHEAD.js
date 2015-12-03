import utils from '../utils';
import Auth from '../auth/checkAuth';
import bucketHead from '../api/bucketHead';
import objectHead from '../api/objectHead';

export default function routeHEAD(req, response, datastore, metastore) {
    utils.normalizeRequest(req);
    Auth.checkAuth(req, function checkAuthRes(err, accessKey) {
        if (err) {
            return utils.errorXmlResponse(response, err);
        }

        if (utils.getResourceNames(req).object === undefined) {
            bucketHead(accessKey, metastore, req, (err) => {
                if (err) {
                    return utils.errorXmlResponse(response, err);
                }
                return utils.okHeaderResponse(response, 200);
            });
        } else {
            objectHead(accessKey, metastore, req,
                    (err, responseMetaHeaders) => {
                        if (err) {
                            return utils.errorXmlResponse(response, err);
                        }
                        utils.buildSuccessResponse(req.lowerCaseHeaders,
                                response, responseMetaHeaders);
                        return response.end();
                    });
        }
    });
}
