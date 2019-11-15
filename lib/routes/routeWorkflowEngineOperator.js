const url = require('url');
const httpProxy = require('http-proxy');

const workflowEngineOperatorProxy = httpProxy.createProxyServer({
    ignorePath: true,
});
const { auth, errors, s3routes } =
    require('arsenal');
const { responseJSONBody } = s3routes.routesUtils;
const vault = require('../auth/vault');
const prepareRequestContexts = require(
'../api/apiUtils/authorization/prepareRequestContexts');
const { config } = require('../Config');
const constants = require('../../constants');

auth.setHandler(vault);

function _decodeURI(uri) {
    // do the same decoding than in S3 server
    return decodeURIComponent(uri.replace(/\+/g, ' '));
}

function _normalizeRequest(req) {
    /* eslint-disable no-param-reassign */
    const parsedUrl = url.parse(req.url, true);
    req.path = _decodeURI(parsedUrl.pathname);
    const pathArr = req.path.split('/');
    req.query = parsedUrl.query;
    req.resourceType = pathArr[3];
    req.bucketName = pathArr[4];
    req.objectKey = pathArr.slice(5).join('/');
    /* eslint-enable no-param-reassign */
}

function routeWorkflowEngineOperator(clientIP, request, response, log) {
    log.debug('routing request', {
        method: 'routeWorkflowEngineOperator',
        url: request.url,
    });
    _normalizeRequest(request);
    const requestContexts = prepareRequestContexts('objectReplicate', request);

    // proxy api requests to Workflow Engine Operator API server
    if (request.resourceType === 'api') {
        if (!config.workflowEngineOperator) {
            log.debug('unable to proxy workflow engine operator request', {
                workflowEngineConfig: config.workflowEngineOperator,
            });
            return responseJSONBody(errors.MethodNotAllowed, null, response,
                log);
        }
        const path = request.url.replace('/_/workflow-engine-operator/api', '/_/');
        const { host, port } = config.workflowEngineOperator;
        const target = `http://${host}:${port}${path}`;
        return auth.server.doAuth(request, log, (err, userInfo) => {
            if (err) {
                log.debug('authentication error', {
                    error: err,
                    method: request.method,
                    bucketName: request.bucketName,
                    objectKey: request.objectKey,
                });
                return responseJSONBody(err, null, response, log);
            }
            // FIXME for now, any authenticated user can access API
            // routes. We should introduce admin accounts or accounts
            // with admin privileges, and restrict access to those
            // only.
            if (userInfo.getCanonicalID() === constants.publicId) {
                log.debug('unauthenticated access to API routes', {
                    method: request.method,
                    bucketName: request.bucketName,
                    objectKey: request.objectKey,
                });
                return responseJSONBody(
                    errors.AccessDenied, null, response, log);
            }
            return workflowEngineOperatorProxy.web(
                request, response, { target }, err => {
                    log.error('error proxying request to api server',
                              { error: err.message });
                    return responseJSONBody(errors.ServiceUnavailable, null,
                                            response, log);
                });
        }, 's3', requestContexts);
    }
    return undefined;
}


module.exports = routeWorkflowEngineOperator;
