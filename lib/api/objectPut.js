import { errors } from 'arsenal';
import { versioning } from 'arsenal';

import data from '../data/wrapper';
import services from '../services';
import aclUtils from '../utilities/aclUtils';
import utils from '../utils';
import { cleanUpBucket } from './apiUtils/bucket/bucketCreation';
import collectCorsHeaders from '../utilities/collectCorsHeaders';
import { dataStore } from './apiUtils/object/storeObject';
import constants from '../../constants';
import { logger } from '../utilities/logger';
import { pushMetric } from '../utapi/utilities';
import metadata from '../metadata/wrapper';
import kms from '../kms/wrapper';
import removeAWSChunked from './apiUtils/object/removeAWSChunked';

function _storeInMDandDeleteData(bucketName, dataGetInfo, cipherBundle,
    metadataStoreParams, dataToDelete, nullVersionParams, deleteLog, callback) {
    if (nullVersionParams) {
        const versioningParams = { versionId: nullVersionParams.nullVersionId };
        const putLog = logger.newRequestLoggerFromSerializedUids(
            deleteLog.getSerializedUids());
        putLog.trace('about to store null version in version history');
        metadata.putObjectMD(bucketName, metadataStoreParams.objectKey,
        nullVersionParams.objectMD, versioningParams, putLog,
        (err, versionId) => {
            if (err) {
                putLog.debug('error from metadata', { error: err });
                return callback(err);
            }
            putLog.trace('null version successfully stored as new version' +
            'in metadata', { versionId });
            return services.metadataStoreObject(bucketName, dataGetInfo,
                cipherBundle, metadataStoreParams, (err, contentMD5,
                newVersionId) => {
                    if (err) {
                        return callback(err);
                    }
                    if (dataToDelete) {
                        data.batchDelete(dataToDelete, deleteLog);
                    }
                    return callback(null, contentMD5, newVersionId);
                });
        });
    }
    return services.metadataStoreObject(bucketName, dataGetInfo,
        cipherBundle, metadataStoreParams, (err, contentMD5, newVersionId) => {
            if (err) {
                return callback(err);
            }
            if (dataToDelete) {
                data.batchDelete(dataToDelete, deleteLog);
            }
            return callback(null, contentMD5, newVersionId);
        });
}

function _getVersioningParams(objectMD, versioningConfig, requestVid) {
    let versioningParams;
    let nullVersionParams;
    let isNull;
    let nullVersionId = objectMD.nullVersionId;

    const errMsg = `Version ${requestVid} does not exist`;
    const versioningError =
        errors.InvalidArgument.customizeDescription(errMsg);
    const nonVersionedObject = objectMD && !objectMD['x-amz-version-id'];
    const currentVersion = objectMD ? objectMD['x-amz-version-id'] : undefined;
    if (versioningConfig) {
        if (requestVid) {
            if (nonVersionedObject && requestVid === 'null') {
                // updating null version
                versioningParams = { versionId: '' };
            } else if (nonVersionedObject) {
                // should not be able to put a specific version that is not
                // 'null' if versioning has never been enabled
                return { versioningError };
            } else if (requestVid === 'null' & !objectMD.isNull) {
                // updating null version if the current version is not null
                if (objectMD.nullVersionId) {
                    versioningParams = { versionId: objectMD.nullVersionId };
                    // TODO: need to delete previous null version
                    // probably if delete version api handles deleting data
                    // in datastore, can use that method -- otherwise have to
                    // get objectMD of nullVersionId to get data.locations
                    // to delete in datastore in _storeIt
                    nullVersionId = undefined;
                } else {
                    return { versioningError };
                }
            } else {
                // in all other cases, we put or overwrite the specified
                // version using the version id that was sent
                versioningParams = { versionId: requestVid };
            }
        } else if (versioningConfig.Status === 'Enabled') {
            versioningParams = { versioning: true };
            if (nonVersionedObject) {
                // if existing object is not versioned, need to send separate
                // request to store null version in version history
                nullVersionParams = {
                    // metadata constant:
                    // references first version before versioning
                    versionId: 'INF',
                    objectMD: Object.assign(objectMD, { isNull: true }),
                };
                // save as the null version id
                nullVersionId = 'INF';
            } else if (objectMD.isNull) {
                // if latest version is null version, need to send separate
                // request to store null version in version history
                nullVersionParams = {
                    versionId: currentVersion,
                    objectMD: Object.assign(objectMD, { isNull: true }),
                };
                nullVersionId = currentVersion;
            }
        } else if (versioningConfig.Status === 'Suspended') {
            // overwrite the master version without generating
            // another version as we don't want to keep this version in the
            // version history (can be called a 'null version')
            versioningParams = { versionId: '' };
            if (objectMD.nullVersionId) {
                // TODO: need to delete previous null version
                // probably if delete version api handles deleting data
                // in datastore, can use that method -- otherwise have to
                // get objectMD of nullVersionId to get data.locations
                // to delete in datastore in _storeIt
                nullVersionId = undefined;
            }
            isNull = true;
        }
    // if no versioning configuration exists, no versioning params to send
    } else {
        versioningParams = {};
    }
    return { versioningParams, isNull, nullVersionParams, nullVersionId };
}

function _storeIt(bucketName, objectKey, objMD, authInfo, canonicalID,
                  cipherBundle, request, streamingV4Params,
                  versioningConfiguration, corsHeaders, log, callback) {
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

    const {
        versioningParams,
        nullVersionParams,
        isNull,
        nullVersionId,
        versioningError,
    } = _getVersioningParams(new versioning.Version(objMD),
        versioningConfiguration,
        request.query ? request.query.versionID : undefined
    );
    if (versioningError) {
        return callback(versioningError);
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
        isNull,
        nullVersionId,
        headers,
        versioningParams,
        log,
    };
    let dataToDelete;
    const versioningEnabled = versioningConfiguration ?
        versioningConfiguration.Status === 'Enabled' : false;
    // only delete data when overwriting an existing object or version
    // - when versioning has never been enabled (overwriting an object)
    // - when versioning is suspended (overwriting the null version)
    // there is no overwriting the data of an existing not-null version
    if (!versioningEnabled && objMD && objMD.location) {
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
        return dataStore(objectKeyContext, cipherBundle, request, size,
            streamingV4Params, log, (err, dataGetInfo, calculatedHash) => {
                if (err) {
                    log.trace('error from data', {
                        error: err,
                        method: 'dataStore',
                    });
                    return callback(err, corsHeaders);
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
                    metadataStoreParams, dataToDelete, nullVersionParams,
                    logger.newRequestLoggerFromSerializedUids(
                        log.getSerializedUids()),
                        (err, contentMD5, versionId) => {
                            if (err) {
                                return callback(err, corsHeaders);
                            }
                            pushMetric('putObject', log, {
                                authInfo,
                                bucket: bucketName,
                                newByteLength: size,
                                oldByteLength: prevContentLen,
                            });
                            return callback(null, corsHeaders, contentMD5,
                                versionId);
                        });
            });
    }
    log.trace('content-length is 0 so only storing metadata', {
        method: 'services.metadataValidateAuthorization',
    });
    metadataStoreParams.contentMD5 = constants.emptyFileMd5;
    const dataGetInfo = null;
    return _storeInMDandDeleteData(bucketName, dataGetInfo, cipherBundle,
        metadataStoreParams, dataToDelete, nullVersionParams,
            logger.newRequestLoggerFromSerializedUids(log
            .getSerializedUids()), (err, contentMD5, newVid) => {
                if (err) {
                    return callback(err, corsHeaders);
                }
                pushMetric('putObject', log, {
                    authInfo,
                    bucket: bucketName,
                    newByteLength: size,
                    oldByteLength: prevContentLen,
                });
                return callback(null, corsHeaders, contentMD5, newVid);
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
        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (err) {
            log.trace('error processing request', {
                error: err,
                method: 'services.metadataValidateAuthorization',
            });
            return callback(err, corsHeaders);
        }
        if (bucket.hasDeletedFlag() &&
            canonicalID !== bucket.getOwner()) {
            log.trace('deleted flag on bucket and request ' +
                'from non-owner account');
            return callback(errors.NoSuchBucket);
        }
        const serverSideEncryption = bucket.getServerSideEncryption();
        const versioningConfig = bucket.getVersioningConfiguration();
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
                            return callback(errors.InternalError,
                                corsHeaders);
                        }
                        if (serverSideEncryption) {
                            return kms.createCipherBundle(
                                serverSideEncryption,
                                log, (err, cipherBundle) => {
                                    if (err) {
                                        return callback(errors.InternalError,
                                            corsHeaders);
                                    }
                                    return _storeIt(bucketName, objectKey,
                                        objMD, authInfo, canonicalID,
                                        cipherBundle, request,
                                        streamingV4Params, versioningConfig,
                                        corsHeaders, log, callback);
                                });
                        }
                        return _storeIt(bucketName, objectKey, objMD,
                                        authInfo, canonicalID, null, request,
                                        streamingV4Params, versioningConfig,
                                        corsHeaders, log, callback);
                    });
        }
        if (serverSideEncryption) {
            return kms.createCipherBundle(
                serverSideEncryption,
                log, (err, cipherBundle) => {
                    if (err) {
                        return callback(errors.InternalError, corsHeaders);
                    }
                    return _storeIt(bucketName, objectKey, objMD,
                                    authInfo, canonicalID, cipherBundle,
                                    request, streamingV4Params,
                                    versioningConfig, corsHeaders, log,
                                    callback);
                });
        }
        return _storeIt(bucketName, objectKey, objMD, authInfo, canonicalID,
                        null, request, streamingV4Params,
                        versioningConfig, corsHeaders, log, callback);
    });
}
