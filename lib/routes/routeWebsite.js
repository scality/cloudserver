import { errors } from 'arsenal';

import api from '../api/api';
import routesUtils from './routesUtils';
import statsReport500 from '../utilities/statsReport500';

export default function routerWebsite(request, response, log, statsClient) {
    log.debug('routing request', { method: 'routerWebsite' });
    // website endpoint only supports GET and HEAD and must have a bucket
    // http://docs.aws.amazon.com/AmazonS3/latest/dev/WebsiteEndpoints.html
    if ((request.method !== 'GET' && request.method !== 'HEAD')
        || !request.bucketName) {
        return routesUtils.errorHtmlResponse(errors.MethodNotAllowed,
            false, request.bucketName, response, log);
    }
    if (request.method === 'GET') {
        return api.callApiMethod('websiteGet', request, log,
            (err, userErrorPageFailure, dataGetInfo, resMetaHeaders,
            redirectInfo, key) => {
                statsReport500(err, statsClient);
                // request being redirected
                if (redirectInfo) {
                    // note that key might have been modified in websiteGet
                    // api to add index document
                    return routesUtils.redirectRequest(redirectInfo,
                        key, request.connection.encrypted,
                        response, request.headers.host, log);
                }
                // user has their own error page
                if (err && dataGetInfo) {
                    return routesUtils.streamUserErrorPage(err, dataGetInfo,
                        response, log);
                }
                // send default error html response
                if (err) {
                    return routesUtils.errorHtmlResponse(err,
                        userErrorPageFailure, request.bucketName,
                        response, log);
                }
                // no error, stream data
                return routesUtils.responseStreamData(null, request.headers,
                    resMetaHeaders, dataGetInfo, response, null, log);
            });
    }
    if (request.method === 'HEAD') {
        return api.callApiMethod('websiteHead', request, log,
        (err, resHeaders) => {
            statsReport500(err, statsClient);
            return routesUtils.responseContentHeaders(err, {}, resHeaders,
                                               response, log);
        });
    }
    return undefined;
}
