const url = require('url');
const { waterfall } = require('async');
const httpProxy = require('http-proxy');
const { auth, errors, s3routes, policies } = require('arsenal');
const { config } = require('../Config');
const constants = require('../../constants');
const vault = require('../auth/vault');

const requestUtils = policies.requestUtils;
const RequestContext = policies.RequestContext;
const { responseJSONBody } = s3routes.routesUtils;
const metadataProxy = httpProxy.createProxyServer({ ignorePath: true });
auth.setHandler(vault);

function _normalizeMetadataRequest(req) {
    /* eslint-disable no-param-reassign */
    const parsedUrl = url.parse(req.url, true);
    req.path = parsedUrl.pathname;
    req.query = parsedUrl.query;
    const pathArr = req.path.split('/');
    req.resourceType = pathArr[3]; // admin, default
    req.generalResource = pathArr[4]; // raft_sessions, buckets
    if (pathArr[5]) {
        req.specificResource = pathArr[5]; // raft session ids, bucket names
    }
    req.subResource = pathArr[6];
    /* eslint-enable no-param-reassign */
}

function routeMetadata(clientIP, request, response, log) {
    // Attach the apiMethod method to the request, so it can used by monitoring in the server
    // eslint-disable-next-line no-param-reassign
    request.apiMethod = 'routeMetadata';

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

    // restrict access to only routes ending in bucket, log or id
    const { resourceType, subResource } = request;
    if (resourceType === 'admin'
        && !['bucket', 'log', 'id'].includes(subResource)) {
        return responseJSONBody(errors.NotImplemented, null, response, log);
    }
    const ip = requestUtils.getClientIp(request, config);
    const requestContexts = [new RequestContext(request.headers, request.query,
        request.generalResource, request.specificResource, ip,
        request.connection.encrypted, request.resourceType, 'metadata')];
    return waterfall([
        next => auth.server.doAuth(request, log, (err, userInfo, authRes) => {
            if (err) {
                log.debug('authentication error', {
                    error: err,
                    method: request.method,
                    bucketName: request.bucketName,
                    objectKey: request.objectKey,
                });
                return next(err);
            }
            // authRes is not defined for account credentials
            if (authRes && !authRes[0].isAllowed) {
                return next(errors.AccessDenied);
            }
            return next(null, userInfo);
        }, 's3', requestContexts),
        (userInfo, next) => {
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
