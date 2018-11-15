const url = require('url');
const { waterfall } = require('async');
const httpProxy = require('http-proxy');
const { auth, errors, s3routes } = require('arsenal');
const { config } = require('../Config');
const constants = require('../../constants');
const vault = require('../auth/vault');
const prepareRequestContexts = require(
    '../api/apiUtils/authorization/prepareRequestContexts');

const { responseJSONBody } = s3routes.routesUtils;
const metadataProxy = httpProxy.createProxyServer({ ignorePath: true });
auth.setHandler(vault);

function _normalizeMetadataRequest(req) {
    /* eslint-disable no-param-reassign */
    const parsedUrl = url.parse(req.url, true);
    req.path = parsedUrl.pathname;
    req.query = parsedUrl.query;
    const pathArr = req.path.split('/');
    req.resourceType = pathArr[3];
    req.bucketName = pathArr[4];
    if (pathArr[5]) {
        req.objectKey = pathArr.slice(5).join('/');
    }
    /* eslint-enable no-param-reassign */
}

function routeMetadata(clientIP, request, response, log) {
    const { bootstrap } = config.bucketd;
    if (bootstrap.length === 0) {
        log.debug('cloudserver is not configured with bucketd', {
            bucketdConfig: config.bucketd,
        });
        return responseJSONBody(errors.ServiceUnavailable, null, response, log);
    }
    log.debug('routing request', { method: 'routeMetadata' });
    log.addDefaultFields({ clientIP, httpMethod: request.method });
    _normalizeMetadataRequest(request);
    const requestContexts = prepareRequestContexts('objectReplicate', request);
    return waterfall([
        next => auth.server.doAuth(request, log, (err, userInfo) => {
            if (err) {
                log.debug('authentication error', {
                    error: err,
                    method: request.method,
                    bucketName: request.bucketName,
                    objectKey: request.objectKey,
                });
            }
            return next(err, userInfo);
        }, 's3', requestContexts),
        (userInfo, next) => {
            // TODO: refactor this for code-reuse in development/8.0
            if (userInfo.getCanonicalID() === constants.publicId) {
                log.debug('unauthenticated access to API routes', {
                    method: request.method,
                    bucketName: request.bucketName,
                    objectKey: request.objectKey,
                });
                return next(errors.AccessDenied);
            }
            const { url } = request;
            const path = url.startsWith('/_/metadata/admin') ?
                url.replace('/_/metadata/admin/', '/_/') :
                url.replace('/_/metadata/', '/');
            // bucketd is always configured on the loopback interface in s3c
            const endpoint = bootstrap[0];
            // TODO: support https bucketd
            const target = `http://${endpoint}${path}`;
            return metadataProxy.web(request, response, { target }, err => {
                if (err) {
                    log.error('error proxying request to metadata admin server',
                          { error: err.message });
                    return next(errors.ServiceUnavailable);
                }
                return next();
            });
        }],
        err => {
            if (err) {
                return responseJSONBody(err, null, response, log);
            }
            log.debug('metadata route response sent successfully',
                { method: request.method,
                    bucketName: request.bucketName,
                    objectKey: request.objectKey });
            return undefined;
        });
}


module.exports = routeMetadata;
