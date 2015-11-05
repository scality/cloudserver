import utils from '../utils';
import { checkAuth } from '../auth/checkAuth';
import initiateMultipartUpload from '../api/initiateMultipartUpload';
import completeMultipartUpload from '../api/completeMultipartUpload';

export default function routePOST(request, response, datastore, metastore) {
    utils.normalizeRequest(request);
    checkAuth(request, function checkAuthRes(err, accessKey) {
        if (err) {
            return utils.errorXmlResponse(response, err);
        }

        if (utils.getResourceNames(request).object === undefined) {
            return utils.errorXmlResponse('InvalidURI');
        } else if (request.query.uploads !== undefined) {
            initiateMultipartUpload(
                accessKey, metastore, request, (err, result) => {
                    if (err) {
                        return utils.errorXmlResponse(response, err);
                    }
                    return utils.okXmlResponse(response, result);
                });
        } else if (request.query.uploadID !== undefined) {
            completeMultipartUpload(
                accessKey, metastore, request, (err, result) => {
                    if (err) {
                        return utils.errorXmlResponse(response, err);
                    }
                    return utils.okXmlResponse(response, result);
                });
        }
    });
}
