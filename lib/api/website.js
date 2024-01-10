const { errors, s3middleware } = require('arsenal');
const validateHeaders = s3middleware.validateConditionalHeaders;

const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const constants = require('../../constants');
const metadata = require('../metadata/wrapper');
const bucketShield = require('./apiUtils/bucket/bucketShield');
const { appendWebsiteIndexDocument, findRoutingRule, extractRedirectInfo } =
    require('./apiUtils/object/websiteServing');
const { isObjAuthorized, isBucketAuthorized } =
    require('./apiUtils/authorization/permissionChecks');
const collectResponseHeaders = require('../utilities/collectResponseHeaders');
const { pushMetric } = require('../utapi/utilities');
const monitoring = require('../utilities/monitoringHandler');

/**
 * _errorActions - take a number of actions once have error getting obj
 * @param {object} err - arsenal errors object
 * @param {string} errorDocument - key to get error document
 * @param {object []} routingRules - array of routingRule objects
 * @param {object} bucket - bucket metadata
 * @param {string} objectKey - object key from request (or as translated in
 * website)
 * @param {object} corsHeaders - CORS-related response headers
 * @param {object} request - normalized request object
 * @param {object} log - Werelogs instance
 * @param {function} callback - callback to function in route
 * @return {undefined}
 */
function _errorActions(err, errorDocument, routingRules,
    bucket, objectKey, corsHeaders, request, log, callback) {
    const bucketName = bucket.getName();
    const errRoutingRule = findRoutingRule(routingRules,
        objectKey, err.code);
    if (errRoutingRule) {
        // route will redirect
        monitoring.promMetrics(
            'GET', bucketName, err.code, 'getObject');
        return callback(err, false, null, corsHeaders, errRoutingRule,
            objectKey);
    }
    if (request.method === 'HEAD') {
        return callback(err, false, null, corsHeaders);
    }
    if (errorDocument) {
        return metadata.getObjectMD(bucketName, errorDocument, {}, log,
            (errObjErr, errObjMD) => {
                if (errObjErr) {
                    // error retrieving error document so return original error
                    // and set boolean of error retrieving user's error document
                    // to true
                    monitoring.promMetrics(
                        'GET', bucketName, err.code, 'getObject');
                    return callback(err, true, null, corsHeaders);
                }
                // return the default error message if the object is private
                // rather than sending a stored error file
                // eslint-disable-next-line no-param-reassign
                request.objectKey = errorDocument;
                if (!isObjAuthorized(bucket, errObjMD, request.apiMethods || 'objectGet',
                    constants.publicId, null, log, request, request.actionImplicitDenies, true)) {
                    log.trace('errorObj not authorized', { error: err });
                    monitoring.promMetrics(
                        'GET', bucketName, err.code, 'getObject');
                    return callback(err, true, null, corsHeaders);
                }
                const dataLocator = errObjMD.location;
                if (errObjMD['x-amz-server-side-encryption']) {
                    for (let i = 0; i < dataLocator.length; i++) {
                        dataLocator[i].masterKeyId =
                            errObjMD['x-amz-server-side-encryption-aws-' +
                                'kms-key-id'];
                        dataLocator[i].algorithm =
                            errObjMD['x-amz-server-side-encryption'];
                    }
                }

                if (errObjMD['x-amz-website-redirect-location']) {
                    const redirectLocation =
                        errObjMD['x-amz-website-redirect-location'];
                    const redirectInfo = { withError: true,
                        location: redirectLocation };
                    log.trace('redirecting to x-amz-website-redirect-location',
                        { location: redirectLocation });
                    return callback(err, false, dataLocator, corsHeaders,
                        redirectInfo, '');
                }

                const responseMetaHeaders = collectResponseHeaders(errObjMD,
                    corsHeaders);
                pushMetric('getObject', log, {
                    bucket: bucketName,
                    newByteLength: responseMetaHeaders['Content-Length'],
                });
                monitoring.promMetrics(
                    'GET', bucketName, err.code, 'getObject');
                return callback(err, false, dataLocator, responseMetaHeaders);
            });
    }
    monitoring.promMetrics(
        'GET', bucketName, err.code, 'getObject');
    return callback(err, false, null, corsHeaders);
}

function capitalize(str) {
    if (!str || typeof str !== 'string') {
        return str;
    }
    return str.charAt(0).toUpperCase() + str.slice(1);
}

/**
 * Callbacks have different signature for GET and HEAD
 * The website function uses GET callback signature
 * This encapsulate HEAD callback to match GET signature
 * @param {function} callback - HEAD callback
 * @returns {function} HEAD callback with GET signature
 */
function callbackGetToHead(callback) {
    return (err, userErrorPageFailure, dataGetInfo,
        resMetaHeaders, redirectInfo, key) =>
        callback(err, resMetaHeaders, redirectInfo, key);
}

/**
 * Website - Common website function for GET and HEAD
 * Gets metadata and object for website or redirects
 * @param {object} request - normalized request object
 * @param {object} log - Werelogs instance
 * @param {function} callback - callback to function in route
 * @return {undefined}
 */
function website(request, log, callback) {
    if (request.method === 'HEAD') {
        // eslint-disable-next-line no-param-reassign
        callback = callbackGetToHead(callback);
    }
    const methodCapitalized = capitalize(request.method);
    log.debug('processing request', { method: `website${methodCapitalized}` });
    const bucketName = request.bucketName;
    const reqObjectKey = request.objectKey ? request.objectKey : '';

    return metadata.getBucket(bucketName, log, (err, bucket) => {
        if (err) {
            log.trace('error retrieving bucket metadata', { error: err });
            monitoring.promMetrics(
                'GET', bucketName, err.code, 'getObject');
            return callback(err, false);
        }
        if (bucketShield(bucket, `object${methodCapitalized}`)) {
            log.trace('bucket in transient/deleted state so shielding');
            monitoring.promMetrics(
                'GET', bucketName, 404, 'getObject');
            return callback(errors.NoSuchBucket, false);
        }
        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        // bucket ACL's do not matter for website head since it is always the
        // head of an object. object ACL's are what matter
        const websiteConfig = bucket.getWebsiteConfiguration();
        if (!websiteConfig) {
            monitoring.promMetrics(
                'GET', bucketName, 404, 'getObject');
            return callback(errors.NoSuchWebsiteConfiguration, false, null,
            corsHeaders);
        }
        // any errors above would be our own created generic error html
        // if have a website config, error going forward would be user's
        // redirect or error page if they set either in the config

        // handle redirect all
        if (websiteConfig.getRedirectAllRequestsTo()) {
            return callback(null, false, null, corsHeaders,
                websiteConfig.getRedirectAllRequestsTo(), reqObjectKey);
        }

        // check whether need to redirect based on key
        const routingRules = websiteConfig.getRoutingRules();
        const keyRoutingRule = findRoutingRule(routingRules, reqObjectKey);

        if (keyRoutingRule) {
            // TODO: optimize by not rerouting if only routing
            // rule is to change out key
            return callback(null, false, null, corsHeaders,
                keyRoutingRule, reqObjectKey);
        }

        appendWebsiteIndexDocument(request, websiteConfig.getIndexDocument());

        /**
         * Recursive function with 1 recursive call to look for index
         * in case of error for potential redirect to folder notation
         * if there is not already an index appended
         * @param {Error} [originalError] - presence of this argument
         * differentiates original user request from recursive call to /index.
         * This error is returned if /index is not found
         * @returns {undefined}
         */
        function runWebsite(originalError) {
            // get object metadata and check authorization and header
            // validation
            return metadata.getObjectMD(bucketName, request.objectKey, {}, log,
                (err, objMD) => {
                    // Note: In case of error, we intentionally send the original
                    // object key to _errorActions as in case of a redirect, we do
                    // not want to append index key to redirect location
                    if (err) {
                        log.trace('error retrieving object metadata',
                        { error: err });
                        monitoring.promMetrics(
                            'GET', bucketName, err.code, 'getObject');
                        let returnErr = err;
                        const bucketAuthorized = isBucketAuthorized(bucket, request.apiMethods || 'bucketGet',
                        constants.publicId, null, log, request, request.actionImplicitDenies, true);
                        // if index object does not exist and bucket is private AWS
                        // returns 403 - AccessDenied error.
                        if (err.is.NoSuchKey && !bucketAuthorized) {
                            returnErr = errors.AccessDenied;
                        }

                        // Check if key is a folder containing index for redirect 302
                        // https://docs.aws.amazon.com/AmazonS3/latest/userguide/IndexDocumentSupport.html
                        if (!originalError && reqObjectKey && !reqObjectKey.endsWith('/')) {
                            appendWebsiteIndexDocument(request, websiteConfig.getIndexDocument(), true);
                            // propagate returnErr as originalError to be used if index is not found
                            return runWebsite(returnErr);
                        }

                        return _errorActions(originalError || returnErr,
                        websiteConfig.getErrorDocument(), routingRules,
                        bucket, reqObjectKey, corsHeaders, request, log,
                        callback);
                    }
                    if (!isObjAuthorized(bucket, objMD, request.apiMethods || 'objectGet',
                        constants.publicId, null, log, request, request.actionImplicitDenies, true)) {
                        const err = errors.AccessDenied;
                        log.trace('request not authorized', { error: err });
                        return _errorActions(err, websiteConfig.getErrorDocument(),
                            routingRules, bucket,
                            reqObjectKey, corsHeaders, request, log, callback);
                    }

                    // access granted to index document, needs a redirect 302
                    // to the original key with trailing /
                    if (originalError) {
                        const redirectInfo = { withError: true,
                            location: `/${reqObjectKey}/` };
                        return callback(errors.Found, false, null, corsHeaders,
                            redirectInfo, '');
                    }

                    const headerValResult = validateHeaders(request.headers,
                        objMD['last-modified'], objMD['content-md5']);
                    if (headerValResult.error) {
                        const err = headerValResult.error;
                        log.trace('header validation error', { error: err });
                        return _errorActions(err, websiteConfig.getErrorDocument(),
                            routingRules, bucket, reqObjectKey,
                            corsHeaders, request, log, callback);
                    }
                    // check if object to serve has website redirect header
                    // Note: AWS prioritizes website configuration rules over
                    // object key's website redirect header, so we make the
                    // check at the end.
                    if (objMD['x-amz-website-redirect-location']) {
                        const redirectLocation =
                            objMD['x-amz-website-redirect-location'];
                        const redirectInfo =
                            extractRedirectInfo(redirectLocation);
                        log.trace('redirecting to x-amz-website-redirect-location',
                            { location: redirectLocation });
                        return callback(null, false, null, corsHeaders,
                            redirectInfo, '');
                    }
                    // got obj metadata, authorized and headers validated,
                    // good to go
                    const responseMetaHeaders = collectResponseHeaders(objMD,
                        corsHeaders);

                    if (request.method === 'HEAD') {
                        pushMetric('headObject', log, { bucket: bucketName });
                        return callback(null, false, null, responseMetaHeaders);
                    }

                    const dataLocator = objMD.location;
                    if (objMD['x-amz-server-side-encryption']) {
                        for (let i = 0; i < dataLocator.length; i++) {
                            dataLocator[i].masterKeyId =
                                objMD['x-amz-server-side-encryption-aws-' +
                                    'kms-key-id'];
                            dataLocator[i].algorithm =
                                objMD['x-amz-server-side-encryption'];
                        }
                    }
                    pushMetric('getObject', log, {
                        bucket: bucketName,
                        newByteLength: responseMetaHeaders['Content-Length'],
                    });
                    monitoring.promMetrics('GET', bucketName, '200',
                        'getObject', responseMetaHeaders['Content-Length']);
                    return callback(null, false, dataLocator, responseMetaHeaders);
                });
        }

        return runWebsite();
    });
}

module.exports = website;
