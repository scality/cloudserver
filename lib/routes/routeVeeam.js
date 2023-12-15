const url = require('url');
const async = require('async');
const vault = require('../auth/vault');
const putVeeamFile = require('./veeam/put');
const getVeeamFile = require('./veeam/get');
const headVeeamFile = require('./veeam/head');
const listVeeamFiles = require('./veeam/list');
const { deleteVeeamFile } = require('./veeam/delete');
const { auth, s3routes, errors } = require('arsenal');
const { _decodeURI, validPath } = require('./veeam/utils');
const { routesUtils } = require('arsenal/build/lib/s3routes');
const { standardMetadataValidateBucket } = require('../metadata/metadataUtils');
const prepareRequestContexts = require('../api/apiUtils/authorization/prepareRequestContexts');

const { responseXMLBody } = s3routes.routesUtils;

auth.setHandler(vault);

const validObjectKeys = [
    `${validPath}system.xml`,
    `${validPath}capacity.xml`,
];

const apiToAction = {
    PUT: 'PutObject',
    GET: 'GetObject',
    HEAD: 'HeadObject',
    DELETE: 'DeleteObject',
    LIST: 'ListObjects',
};

const routeMap = {
    GET: getVeeamFile,
    PUT: putVeeamFile,
    HEAD: headVeeamFile,
    DELETE: deleteVeeamFile,
    LIST: listVeeamFiles,
};

/**
 * Validator for the Veeam12 custom routes. Ensures that bucket name and
 * object name are correct, and that the bucket exists in the DB.
 * @param {string} bucketName  - name of the bucket
 * @param {string} objectKey - key of the object
 * @param {array | null} requestQueryParams - request query parameters
 * @param {string} method - HTTP verb
 * @param {object} log - request logger
 * @returns {Error | undefined} error or undefined
 */
function checkBucketAndKey(bucketName, objectKey, requestQueryParams, method, log) {
    // In case bucket name is not specified and the request contains an
    // object key or is not a GET, then the bucket name is mandatory.
    // Reject the request in this case.
    if (!bucketName && !(method === 'GET' && !objectKey)) {
        log.debug('empty bucket name', { method: 'checkBucketAndKey' });
        return errors.MethodNotAllowed;
    }
    if (typeof bucketName !== 'string' || routesUtils.isValidBucketName(bucketName, []) === false) {
        log.debug('invalid bucket name', { bucketName });
        if (method === 'DELETE') {
            return errors.NoSuchBucket;
        }
        return errors.InvalidBucketName;
    }
    if (method !== 'LIST') {
        // Reject any unsupported request, but allow downloads and deletes from UI
        // Download relies on GETs calls with auth in query parameters, that can be
        // checked if 'X-Amz-Credential' is included.
        // Deletion requires that the tags of the object are returned.
        if (requestQueryParams && Object.keys(requestQueryParams).length > 0
            && !(method === 'GET' && (requestQueryParams['X-Amz-Credential'] || ('tagging' in requestQueryParams)))) {
            return errors.InvalidRequest
                .customizeDescription('The Veeam SOSAPI folder does not support this action.');
        }
        if (typeof objectKey !== 'string' || !validObjectKeys.includes(objectKey)) {
            log.debug('invalid object name', { objectKey });
            return errors.InvalidArgument;
        }
    }
    return undefined;
}

/**
 * Query the authorization service for the request, and extract the bucket
 * and, if applicable, object metadata according to the request method.
 *
 * @param {object} request - incoming request
 * @param {object} response - response object
 * @param {string} api - HTTP verb
 * @param {object} log - logger instance
 * @param {function} callback -
 * @returns {undefined}
 */
function authorizationMiddleware(request, response, api, log, callback) {
    if (!api) {
        return responseXMLBody(errors.AccessDenied, null, response, log);
    }
    const requestContexts = prepareRequestContexts(api, request);
    return async.waterfall([
        next => auth.server.doAuth(request, log, (err, userInfo, authorizationResults, streamingV4Params) => {
            if (err) {
                log.debug('authentication error', {
                    error: err,
                    method: request.method,
                    bucketName: request.bucketName,
                    objectKey: request.objectKey,
                });
            }
            /* eslint-disable no-param-reassign */
            request.authorizationResults = authorizationResults;
            request.streamingV4Params = streamingV4Params;
            /* eslint-enable no-param-reassign */
            return next(err, userInfo);
        }, 's3', requestContexts),
        (userInfo, next) => {
            // Ensure only supported HTTP verbs and actions are called,
            // otherwise deny access
            const requestType = apiToAction[api];
            if (!requestType) {
                return next(errors.AccessDenied);
            }
            const mdValParams = {
                bucketName: request.bucketName,
                authInfo: userInfo,
                requestType,
                request,
            };
            return next(null, mdValParams);
        },
        (mdValParams, next) => standardMetadataValidateBucket(mdValParams, request.actionImplicitDenies, log, next),
    ], (err, bucketMd) => {
        if (err || !bucketMd) {
            return responseXMLBody(err, null, response, log);
        }
        return callback(request, response, bucketMd, log);
    });
}

function _normalizeVeeamRequest(req) {
    /* eslint-disable no-param-reassign */
    // Rewriting the URL is needed for the V4 signature check: the initial
    // request targets https://s3.subdomain/bucketName/objectKey, but the
    // custom ingresses and/or nginx configuration for the UI will redirect this
    // call to .../_/veeam/bucketName/objectKey. We need to revert the custom
    // path only used for routing before computing the V4 signature.
    req.url = req.url.replace('/_/veeam', '');
    // Assign multiple common (extracted) parameters to the request object
    const parsedUrl = url.parse(req.url, true);
    req.path = _decodeURI(parsedUrl.pathname);
    const pathArr = req.path.split('/');
    req.query = parsedUrl.query;
    req.bucketName = pathArr[1];
    req.objectKey = pathArr.slice(2).join('/');
    const contentLength = req.headers['x-amz-decoded-content-length'] ?
        req.headers['x-amz-decoded-content-length'] :
        req.headers['content-length'];
    req.parsedContentLength =
        Number.parseInt(contentLength?.toString() ?? '', 10);
    /* eslint-enable no-param-reassign */
}

/**
 * Ensure only supported methods are supported, otherwise, return an error
 * @param {string} reqMethod - the HTTP verb of the request
 * @param {string} reqQuery - request query
 * @param {object} reqHeaders - request headers
 * @returns {object} - method or error
 */
function checkUnsupportedRoutes(reqMethod, reqQuery, reqHeaders) {
    const method = routeMap[reqMethod];
    if (!method || (!reqQuery && !reqHeaders)) {
        return { error: errors.MethodNotAllowed };
    }
    return { method };
}

/**
 * Router for the Veeam custom files
 * @param {string} clientIP - client IP address
 * @param {object} request - request object
 * @param {object} response - response object
 * @param {object} log - requets logger
 * @returns {undefined}
 */
function routeVeeam(clientIP, request, response, log) {
    // Attach the apiMethod method to the request, so it can used by monitoring in the server
    // eslint-disable-next-line no-param-reassign
    request.apiMethod = 'routeVeeam';
    _normalizeVeeamRequest(request);

    log.info('routing request', {
        method: 'routeVeeam',
        url: request.url,
        clientIP,
        resourceType: request.resourceType,
        subResource: request.subResource,
    });

    // Rewrite action to LIST for list-objects
    const requestMethod = request.method === 'GET' && !request.objectKey ? 'LIST' : request.method;
    const { error, method } = checkUnsupportedRoutes(requestMethod, request.query, request.headers);

    if (error) {
        log.error('error validating route or uri params', { error });
        return responseXMLBody(error, '', response, log);
    }
    const bucketOrKeyError = checkBucketAndKey(
        request.bucketName, request.objectKey, request.query, requestMethod, log);

    if (bucketOrKeyError) {
        log.error('error with bucket or key value',
            { error: bucketOrKeyError });
        return routesUtils.responseXMLBody(bucketOrKeyError, null, response, log);
    }
    return authorizationMiddleware(request, response, requestMethod, log, method);
}

module.exports = {
    routeVeeam,
    checkUnsupportedRoutes,
    _normalizeVeeamRequest,
    authorizationMiddleware,
    checkBucketAndKey,
    validObjectKeys,
};
