import { errors } from 'arsenal';

import api from '../api/api';
import pushMetrics from '../utilities/pushMetrics';
import routesUtils from './routesUtils';
import statsReport500 from '../utilities/statsReport500';

export default function routerWebsite(request, response, log, utapi,
    statsClient) {
    log.debug('routing request', { method: 'routerWebsite' });
    // website endpoint only supports GET and HEAD and must have a bucket
    // http://docs.aws.amazon.com/AmazonS3/latest/dev/WebsiteEndpoints.html
    if ((request.method !== 'GET' && request.method !== 'HEAD')
        || !request.bucketName) {
        return routesUtils.errorHtmlResponse(errors.MethodNotAllowed,
            false, request.bucketName, response, log);
    }
    if (request.method === 'GET') {
        // TODO: here or in api, handle 500 status check
        // and pushMetrics to utapi.
        return api.callApiMethod('websiteGet', request, log,
            (err, userErrorPageFailure, dataGetInfo, resMetaHeaders,
            redirectInfo, key) => {
                let contentLength = 0;
                if (resMetaHeaders && resMetaHeaders['Content-Length']) {
                    contentLength = resMetaHeaders['Content-Length'];
                }
                statsReport500(err, statsClient);
                pushMetrics(err, log, utapi, 'objectGet', request.bucketName,
                    contentLength);
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
        // TODO: here or in api, handle 500 status check
        // and pushMetrics to utapi.
        return api.callApiMethod('websiteHead', request, log,
        (err, resHeaders) => {
            statsReport500(err, statsClient);
            pushMetrics(err, log, utapi, 'objectHead', request.bucketName);
            return routesUtils.responseContentHeaders(err, {}, resHeaders,
                                               response, log);
        });
    }
    return undefined;
}
