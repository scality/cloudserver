import { errors } from 'arsenal';

import data from '../data/wrapper';
import services from '../services';
import aclUtils from '../utilities/aclUtils';
import utils from '../utils';
import { cleanUpBucket } from './apiUtils/bucket/bucketCreation';
import { dataStore } from './apiUtils/object/storeObject';
import constants from '../../constants';
import { logger } from '../utilities/logger';
import { pushMetric } from '../utapi/utilities';
import kms from '../kms/wrapper';
import removeAWSChunked from './apiUtils/object/removeAWSChunked';

function _storeInMDandDeleteData(bucketName, dataGetInfo, cipherBundle,
    metadataStoreParams, dataToDelete, deleteLog, callback) {
    services.metadataStoreObject(bucketName, dataGetInfo,
        cipherBundle, metadataStoreParams, (err, contentMD5) => {
            if (err) {
                return callback(err);
            }
            if (dataToDelete) {
                data.batchDelete(dataToDelete, deleteLog);
            }
            return callback(null, contentMD5);
        });
}

function _storeIt(bucket, objectKey, objMD, authInfo, canonicalID,
                  cipherBundle, request, streamingV4Params, log, callback) {
    const size = request.parsedContentLength;

    const websiteRedirectHeader =
        request.headers['x-amz-website-redirect-location'];
    if (!utils.validateWebsiteHeader(websiteRedirectHeader)) {
        const err = errors.InvalidRedirectLocation;
        log.debug('invalid x-amz-website-redirect-location' +
            `value ${websiteRedirectHeader}`, { error: err });
        return callback(err);
    }

    const metaHeaders = utils.getMetaHeaders(request.headers);
    log.trace('meta headers', { metaHeaders, method: 'objectPut' });
    const bucketName = bucket.getName();
    const objectKeyContext = {
        bucketName,
        owner: canonicalID,
        namespace: request.namespace,
    };
    // If the request was made with a pre-signed url, the x-amz-acl 'header'
    // might be in the query string rather than the actual headers so include
    // it here
    const headers = request.headers;
    if (request.query && request.query['x-amz-acl']) {
        headers['x-amz-acl'] = request.query['x-amz-acl'];
    }
    const metadataStoreParams = {
        objectKey,
        authInfo,
        metaHeaders,
        size,
        contentType: request.headers['content-type'],
        cacheControl: request.headers['cache-control'],
        contentDisposition: request.headers['content-disposition'],
        contentEncoding:
            removeAWSChunked(request.headers['content-encoding']),
        expires: request.headers.expires,
        headers,
        log,
    };
    let dataToDelete;
    if (objMD && objMD.location) {
        dataToDelete = Array.isArray(objMD.location) ?
            objMD.location : [objMD.location];
    }

    // null - new object
    // 0 or > 0 - existing object with content-length 0 or greater than 0
    const prevContentLen = objMD && objMD['content-length'] !== undefined ?
        objMD['content-length'] : null;
    if (size !== 0) {
        log.trace('storing object in data', {
            method: 'services.metadataValidateAuthorization',
        });
        const backendInfo = {
            objectLocationConstraint: request
                .headers['x-amz-meta-scal-location-constraint'],
            bucketLocationConstraint: bucket.getLocationConstraint(),
            requestEndpoint: request.parsedHost,
        };
        return dataStore(objectKeyContext, cipherBundle, request, size,
            streamingV4Params, backendInfo, log,
            (err, dataGetInfo, calculatedHash) => {
                if (err) {
                    log.trace('error from data', {
                        error: err,
                        method: 'dataStore',
                    });
                    return callback(err);
                }
                // So that data retrieval information for MPU's and
                // regular puts are stored in the same data structure,
                // place the retrieval info here into a single element array
                const dataGetInfoArr = [{
                    key: dataGetInfo.key,
                    size,
                    start: 0,
                    dataStoreName: dataGetInfo.dataStoreName,
                }];
                if (cipherBundle) {
                    dataGetInfoArr[0].cryptoScheme = cipherBundle.cryptoScheme;
                    dataGetInfoArr[0].cipheredDataKey =
                        cipherBundle.cipheredDataKey;
                }
                metadataStoreParams.contentMD5 = calculatedHash;
                return _storeInMDandDeleteData(
                    bucketName, dataGetInfoArr, cipherBundle,
                    metadataStoreParams, dataToDelete,
                    logger.newRequestLoggerFromSerializedUids(
                        log.getSerializedUids()), (err, contentMD5) => {
                            if (err) {
                                return callback(err);
                            }
                            pushMetric('putObject', log, {
                                authInfo,
                                bucket: bucketName,
                                newByteLength: size,
                                oldByteLength: prevContentLen,
                            });
                            return callback(null, contentMD5, prevContentLen);
                        });
            });
    }
    log.trace('content-length is 0 so only storing metadata', {
        method: 'services.metadataValidateAuthorization',
    });
    metadataStoreParams.contentMD5 = constants.emptyFileMd5;
    const dataGetInfo = null;
    return _storeInMDandDeleteData(bucketName, dataGetInfo, cipherBundle,
        metadataStoreParams, dataToDelete,
            logger.newRequestLoggerFromSerializedUids(log
            .getSerializedUids()), (err, contentMD5) => {
                if (err) {
                    return callback(err);
                }
                pushMetric('putObject', log, {
                    authInfo,
                    bucket: bucketName,
                    newByteLength: size,
                    oldByteLength: prevContentLen,
                });
                return callback(null, contentMD5, prevContentLen);
            });
}


/**
 * PUT Object in the requested bucket. Steps include:
 * validating metadata for authorization, bucket and object existence etc.
 * store object data in datastore upon successful authorization
 * store object location returned by datastore and
 * object's (custom) headers in metadata
 * return the result in final callback
 *
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {request} request - request object given by router,
 *                            includes normalized headers
 * @param {object | undefined } streamingV4Params - if v4 auth,
 * object containing accessKey, signatureFromRequest, region, scopeDate,
 * timestamp, and credentialScope
 * (to be used for streaming v4 auth if applicable)
 * @param {object} log - the log request
 * @param {Function} callback - final callback to call with the result
 * @return {undefined}
 */
export default
function objectPut(authInfo, request, streamingV4Params, log, callback) {
    log.debug('processing request', { method: 'objectPut' });
    if (!aclUtils.checkGrantHeaderValidity(request.headers)) {
        log.trace('invalid acl header');
        return callback(errors.InvalidArgument);
    }
    const bucketName = request.bucketName;
    const objectKey = request.objectKey;
    const valParams = {
        authInfo,
        bucketName,
        objectKey,
        requestType: 'objectPut',
        log,
    };
    const canonicalID = authInfo.getCanonicalID();
    log.trace('owner canonicalID to send to data', { canonicalID });

    return services.metadataValidateAuthorization(valParams, (err, bucket,
        objMD) => {
        if (err) {
            log.trace('error processing request', {
                error: err,
                method: 'services.metadataValidateAuthorization',
            });
            return callback(err);
        }
        if (bucket.hasDeletedFlag() &&
            canonicalID !== bucket.getOwner()) {
            log.trace('deleted flag on bucket and request ' +
                'from non-owner account');
            return callback(errors.NoSuchBucket);
        }
        const serverSideEncryption = bucket.getServerSideEncryption();
        if (bucket.hasTransientFlag() ||
            bucket.hasDeletedFlag()) {
            log.trace('transient or deleted flag so cleaning up bucket');
            return cleanUpBucket(bucket,
                    canonicalID, log, err => {
                        if (err) {
                            log.debug('error cleaning up bucket with flag',
                            { error: err,
                            transientFlag:
                                bucket.hasTransientFlag(),
                            deletedFlag:
                                bucket.hasDeletedFlag(),
                            });
                            // To avoid confusing user with error
                            // from cleaning up
                            // bucket return InternalError
                            return callback(errors.InternalError);
                        }
                        if (serverSideEncryption) {
                            return kms.createCipherBundle(
                                serverSideEncryption,
                                log, (err, cipherBundle) => {
                                    if (err) {
                                        return callback(errors.InternalError);
                                    }
                                    return _storeIt(bucket, objectKey,
                                        objMD, authInfo, canonicalID,
                                        cipherBundle, request,
                                        streamingV4Params, log, callback);
                                });
                        }
                        return _storeIt(bucket, objectKey, objMD,
                                        authInfo, canonicalID, null, request,
                                        streamingV4Params, log, callback);
                    });
        }
        if (serverSideEncryption) {
            return kms.createCipherBundle(
                serverSideEncryption,
                log, (err, cipherBundle) => {
                    if (err) {
                        return callback(errors.InternalError);
                    }
                    return _storeIt(bucket, objectKey, objMD,
                                    authInfo, canonicalID, cipherBundle,
                                    request, streamingV4Params, log, callback);
                });
        }
        return _storeIt(bucket, objectKey, objMD, authInfo, canonicalID,
                        null, request, streamingV4Params, log, callback);
    });
}
