const { errors, s3middleware } = require('arsenal');
const validateHeaders = s3middleware.validateConditionalHeaders;

const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const constants = require('../../constants');
const metadata = require('../metadata/wrapper');
const bucketShield = require('./apiUtils/bucket/bucketShield');
const { findRoutingRule, extractRedirectInfo } =
    require('./apiUtils/object/websiteServing');
const collectResponseHeaders = require('../utilities/collectResponseHeaders');
const { pushMetric } = require('../utapi/utilities');
const monitoring = require('../utilities/monitoringHandler');
const { isBucketAuthorized, isObjAuthorized } =
    require('./apiUtils/authorization/permissionChecks');


/**
 * _errorActions - take a number of actions once have error getting obj
 * @param {object} err - arsenal errors object
 * @param {object []} routingRules - array of routingRule objects
 * @param {string} objectKey - object key from request (or as translated in
 * websiteGet)
 * @param {object} corsHeaders - CORS-related response headers
 * @param {object} log - Werelogs instance
 * @param {function} callback - callback to function in route
 * @return {undefined}
 */
function _errorActions(err, routingRules, objectKey, corsHeaders, log,
    callback) {
    const errRoutingRule = findRoutingRule(routingRules, objectKey, err.code);
    if (errRoutingRule) {
        // route will redirect
        return callback(err, corsHeaders, errRoutingRule, objectKey);
    }
    return callback(err, corsHeaders);
}


/**
 * HEAD Website - Gets metadata for object for website or redirects
 * @param {object} request - normalized request object
 * @param {object} log - Werelogs instance
 * @param {function} callback - callback to function in route
 * @return {undefined}
 */
function websiteHead(request, log, callback) {
    log.debug('processing request', { method: 'websiteHead' });
    const bucketName = request.bucketName;
    const reqObjectKey = request.objectKey ? request.objectKey : '';
    let objectKey = reqObjectKey;

    return metadata.getBucket(bucketName, log, (err, bucket) => {
        if (err) {
            log.trace('error retrieving bucket metadata', { error: err });
            monitoring.promMetrics(
                'HEAD', bucketName, err.code, 'headObject');
            return callback(err);
        }
        if (bucketShield(bucket, 'objectHead')) {
            log.trace('bucket in transient/deleted state so shielding');
            monitoring.promMetrics(
                'HEAD', bucketName, 404, 'headObject');
            return callback(errors.NoSuchBucket);
        }
        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        // bucket ACL's do not matter for website head since it is always the
        // head of an object. object ACL's are what matter
        const websiteConfig = bucket.getWebsiteConfiguration();
        if (!websiteConfig) {
            monitoring.promMetrics(
                'HEAD', bucketName, 404, 'headObject');
            return callback(errors.NoSuchWebsiteConfiguration);
        }
        // any errors above would be generic header error response
        // if have a website config, error going forward could be redirect
        // if a redirect rule for error is in config

        // handle redirect all
        if (websiteConfig.getRedirectAllRequestsTo()) {
            return callback(null, corsHeaders,
                websiteConfig.getRedirectAllRequestsTo(), objectKey);
        }

        // find index document if "directory" sent in request
        if (reqObjectKey.endsWith('/')) {
            objectKey += websiteConfig.getIndexDocument();
        }
        // find index document if no key provided
        if (reqObjectKey === '') {
            objectKey = websiteConfig.getIndexDocument();
        }
        // check whether need to redirect based on key
        const routingRules = websiteConfig.getRoutingRules();

        const keyRoutingRule = findRoutingRule(routingRules, objectKey);

        if (keyRoutingRule) {
            return callback(null, corsHeaders, keyRoutingRule, reqObjectKey);
        }

        // get object metadata and check authorization and header
        // validation
        return metadata.getObjectMD(bucketName, objectKey, {}, log,
            (err, objMD) => {
                // Note: In case of error, we intentionally send the original
                // object key to _errorActions as in case of a redirect, we do
                // not want to append index key to redirect location
                if (err) {
                    log.trace('error retrieving object metadata',
                    { error: err });
                    let returnErr = err;
                    const bucketAuthorized = isBucketAuthorized(bucket,
                      'bucketGet', constants.publicId, null, log, request);
                    // if index object does not exist and bucket is private AWS
                    // returns 403 - AccessDenied error.
                    if (err === errors.NoSuchKey && !bucketAuthorized) {
                        returnErr = errors.AccessDenied;
                    }
                    return _errorActions(returnErr, routingRules,
                        reqObjectKey, corsHeaders, log, callback);
                }
                if (!isObjAuthorized(bucket, objMD, 'objectGet',
                    constants.publicId, null, log, request)) {
                    const err = errors.AccessDenied;
                    log.trace('request not authorized', { error: err });
                    return _errorActions(err, routingRules, reqObjectKey,
                        corsHeaders, log, callback);
                }

                const headerValResult = validateHeaders(request.headers,
                    objMD['last-modified'], objMD['content-md5']);
                if (headerValResult.error) {
                    const err = headerValResult.error;
                    log.trace('header validation error', { error: err });
                    return _errorActions(err, routingRules, reqObjectKey,
                        corsHeaders, log, callback);
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
                    return callback(null, corsHeaders, redirectInfo, '');
                }

                // got obj metadata, authorized and headers validated,
                // good to go
                const responseMetaHeaders = collectResponseHeaders(objMD,
                    corsHeaders);
                pushMetric('headObject', log, {
                    bucket: bucketName,
                });
                monitoring.promMetrics(
                    'HEAD', bucketName, '200', 'headObject');
                return callback(null, responseMetaHeaders);
            });
    });
}

module.exports = websiteHead;
