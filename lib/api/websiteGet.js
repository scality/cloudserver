import { errors } from 'arsenal';

import constants from '../../constants';
import metadata from '../metadata/wrapper';
import bucketShield from './apiUtils/bucket/bucketShield';
import { findRoutingRule } from './apiUtils/object/websiteServing';
import { isObjAuthorized } from './apiUtils/authorization/aclChecks';
import collectResponseHeaders from '../utilities/collectResponseHeaders';
import validateHeaders from '../utilities/validateHeaders';
import { pushMetric } from '../utapi/utilities';

/**
 * _errorActions - take a number of actions once have error getting obj
 * @param {object} err - arsenal errors object
 * @param {string} errorDocument - key to get error document
 * @param {object []} routingRules - array of routingRule objects
 * @param {string} bucketName - bucket name from request
 * @param {string} objectKey - object key from request (or as translated in
 * websiteGet)
 * @param {object} log - Werelogs instance
 * @param {function} callback - callback to function in route
 * @return {undefined}
 */
function _errorActions(err, errorDocument, routingRules,
    bucketName, objectKey, log, callback) {
    const errRoutingRule = findRoutingRule(routingRules,
        objectKey, err.code);
    if (errRoutingRule) {
        // route will redirect
        return callback(err, false, null, null, errRoutingRule, objectKey);
    }
    if (errorDocument) {
        return metadata.getObjectMD(bucketName, errorDocument, log,
            (errObjErr, errObjMD) => {
                if (errObjErr) {
                    // error retrieving error document so return original error
                    // and set boolean of error retrieving user's error document
                    // to true
                    return callback(err, true);
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
                const responseMetaHeaders = collectResponseHeaders(errObjMD);
                pushMetric('getObject', log, {
                    bucket: bucketName,
                    newByteLength: responseMetaHeaders['Content-Length'],
                });
                return callback(err, false, dataLocator, responseMetaHeaders);
            });
    }
    return callback(err, false);
}

/**
 * GET Website - Gets object for website or redirects
 * @param {object} request - normalized request object
 * @param {object} log - Werelogs instance
 * @param {function} callback - callback to function in route
 * @return {undefined}
 */
export default
function websiteGet(request, log, callback) {
    log.debug('processing request', { method: 'websiteGet' });
    const bucketName = request.bucketName;
    const reqObjectKey = request.objectKey ? request.objectKey : '';
    let objectKey = reqObjectKey;

    return metadata.getBucket(bucketName, log, (err, bucket) => {
        if (err) {
            log.trace('error retrieving bucket metadata', { error: err });
            return callback(err, false);
        }
        if (bucketShield(bucket, 'objectGet')) {
            log.trace('bucket in transient/deleted state so shielding');
            return callback(errors.NoSuchBucket, false);
        }
        // bucket ACL's do not matter for website get since it is always the
        // get of an object. object ACL's are what matter
        const websiteConfig = bucket.getWebsiteConfiguration();
        if (!websiteConfig) {
            return callback(errors.NoSuchWebsiteConfiguration, false);
        }
        // any errors above would be our own created generic error html
        // if have a website config, error going forward would be user's
        // redirect or error page if they set either in the config

        // handle redirect all
        if (websiteConfig.getRedirectAllRequestsTo()) {
            return callback(null, false, null, null,
                websiteConfig.getRedirectAllRequestsTo(), objectKey);
        }

        // check whether need to redirect based on key
        const routingRules = websiteConfig.getRoutingRules();
        const keyRoutingRule = findRoutingRule(routingRules, objectKey);

        if (keyRoutingRule) {
            // TODO: optimize by not rerouting if only routing
            // rule is to change out key
            return callback(null, false, null, null,
                keyRoutingRule, objectKey);
        }

        // find index document if "directory" sent in request
        if (reqObjectKey.endsWith('/')) {
            objectKey = objectKey + websiteConfig.getIndexDocument();
        }
        // find index document if no key provided
        if (reqObjectKey === '') {
            objectKey = websiteConfig.getIndexDocument();
        }

        // get object metadata and check authorization and header
        // validation
        return metadata.getObjectMD(bucketName, objectKey, log,
            (err, objMD) => {
                // Note: In case of error, we intentionally send the original
                // object key to _errorActions as in case of a redirect, we do
                // not want to append index key to redirect location
                if (err) {
                    log.trace('error retrieving object metadata',
                    { error: err });
                    let returnErr = err;
                    // AWS returns AccessDenied instead of NoSuchKey
                    if (err === errors.NoSuchKey) {
                        returnErr = errors.AccessDenied;
                    }
                    return _errorActions(returnErr,
                      websiteConfig.getErrorDocument(), routingRules,
                      bucketName, reqObjectKey, log, callback);
                }
                if (!isObjAuthorized(bucket, objMD, 'objectGet',
                    constants.publicId)) {
                    const err = errors.AccessDenied;
                    log.trace('request not authorized', { error: err });
                    return _errorActions(err, websiteConfig.getErrorDocument(),
                        routingRules, bucketName, reqObjectKey, log,
                        callback);
                }

                const headerValResult = validateHeaders(objMD, request.headers);
                if (headerValResult.error) {
                    const err = headerValResult.error;
                    log.trace('header validation error', { error: err });
                    return _errorActions(err, websiteConfig.getErrorDocument(),
                        routingRules, bucketName, reqObjectKey, log,
                        callback);
                }
                // got obj metadata, authorized and headers validated,
                // good to go
                const responseMetaHeaders = collectResponseHeaders(objMD);
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
                return callback(null, false, dataLocator, responseMetaHeaders);
            });
    });
}
