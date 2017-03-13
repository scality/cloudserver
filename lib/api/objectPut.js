import { errors, versioning } from 'arsenal';
import async from 'async';

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
import kms from '../kms/wrapper';
import removeAWSChunked from './apiUtils/object/removeAWSChunked';
import metadata from '../metadata/wrapper';

const VID = versioning.VersionID;


function _storeInMDandDeleteData(bucketName, dataGetInfo, cipherBundle,
    metadataStoreParams, dataToDelete, deleteLog, callback) {
    services.metadataStoreObject(bucketName, dataGetInfo,
        cipherBundle, metadataStoreParams, (err, res) => {
            if (err) {
                return callback(err);
            }
            if (dataToDelete) {
                data.batchDelete(dataToDelete, deleteLog);
            }
            return callback(null, res);
        });
}

function createAndStoreObject(bucketName, bucketMD, objectKey, objMD, authInfo,
        canonicalID, cipherBundle, request, streamingV4Params, log, callback) {
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
    const metadataStoreParams = {
        objectKey, authInfo, metaHeaders, size,
        contentType: request.headers['content-type'],
        cacheControl: request.headers['cache-control'],
        contentDisposition: request.headers['content-disposition'],
        contentEncoding: removeAWSChunked(request.headers['content-encoding']),
        expires: request.headers.expires, headers, log,
        isDeleteMarker: request.isDeleteMarker,
    };
    let dataGetInfoArr = undefined;
    let dataToDelete = undefined;
    if (objMD && objMD.location) {
        dataToDelete = Array.isArray(objMD.location) ?
            objMD.location : [objMD.location];
    }
    const reqVersionId = request.query ? request.query.versionId : undefined;

    // null - new object
    // 0 or > 0 - existing object with content-length 0 or greater than 0
    const requestLogger =
        logger.newRequestLoggerFromSerializedUids(log.getSerializedUids());
    return async.waterfall([
        callback => {
            if (size === 0) {
                metadataStoreParams.contentMD5 = constants.emptyFileMd5;
                return callback(null, null, null);
            }
            return dataStore(objectKeyContext, cipherBundle, request, size,
                    streamingV4Params, log, callback);
        },
        (dataGetInfo, calculatedHash, callback) => {
            if (dataGetInfo === null || dataGetInfo === undefined) {
                return callback(null, null);
            }
            // So that data retrieval information for MPU's and
            // regular puts are stored in the same data structure,
            // place the retrieval info here into a single element array
            const { key, dataStoreName } = dataGetInfo;
            const dataGetInfoArr = [{ key, size, start: 0, dataStoreName }];
            if (cipherBundle) {
                dataGetInfoArr[0].cryptoScheme = cipherBundle.cryptoScheme;
                dataGetInfoArr[0].cipheredDataKey =
                    cipherBundle.cipheredDataKey;
            }
            metadataStoreParams.contentMD5 = calculatedHash;
            return callback(null, dataGetInfoArr);
        },
        (infoArr, callback) => {
            dataGetInfoArr = infoArr;
            return services.versioningPreprocessing(bucketName, bucketMD,
                    metadataStoreParams.objectKey, objMD, reqVersionId, log,
                    callback);
        },
        (options, callback) => {
            if (!options.deleteNullVersionData) {
                return callback(null, options);
            }
            const params = { versionId: options.nullVersionId };
            return metadata.getObjectMD(bucketName, objectKey,
                params, log, (err, nullObjMD) => {
                    if (nullObjMD.location) {
                        dataToDelete = Array.isArray(nullObjMD.location) ?
                            nullObjMD.location : [nullObjMD.location];
                    }
                    return callback(null, options);
                });
        },
        (options, callback) => {
            metadataStoreParams.versionId = options.versionId;
            metadataStoreParams.versioning = options.versioning;
            metadataStoreParams.isNull = options.isNull;
            metadataStoreParams.nullVersionId = options.nullVersionId;
            return _storeInMDandDeleteData(bucketName, dataGetInfoArr,
                    cipherBundle, metadataStoreParams,
                    options.deleteData ? dataToDelete : undefined,
                    requestLogger, callback);
        },
    ], callback);
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
function objectPut(authInfo, request, streamingV4Params, log, callback) {
    log.debug('processing request', { method: 'objectPut' });
    if (!aclUtils.checkGrantHeaderValidity(request.headers)) {
        log.trace('invalid acl header');
        return callback(errors.InvalidArgument);
    }
    const bucketName = request.bucketName;
    const objectKey = request.objectKey;
    const requestType = 'objectPut';
    const valParams = { authInfo, bucketName, objectKey, requestType, log };
    const canonicalID = authInfo.getCanonicalID();
    log.trace('owner canonicalID to send to data', { canonicalID });

    return services.metadataValidateAuthorization(valParams,
    (err, bucket, objMD) => {
        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (err) {
            log.trace('error processing request', {
                error: err,
                method: 'services.metadataValidateAuthorization',
            });
            return callback(err, null, corsHeaders);
        }
        if (bucket.hasDeletedFlag() && canonicalID !== bucket.getOwner()) {
            log.trace('deleted flag on bucket and request ' +
                'from non-owner account');
            return callback(errors.NoSuchBucket);
        }
        return async.waterfall([
            callback => {
                if (bucket.hasTransientFlag() || bucket.hasDeletedFlag()) {
                    return cleanUpBucket(bucket, canonicalID, log, callback);
                }
                return callback();
            },
            callback => {
                const serverSideEncryption = bucket.getServerSideEncryption();
                if (serverSideEncryption) {
                    return kms.createCipherBundle(
                            serverSideEncryption, log, callback);
                }
                return callback(null, null);
            },
            (cipherBundle, callback) => createAndStoreObject(bucketName,
                bucket, objectKey, objMD, authInfo, canonicalID, cipherBundle,
                request, streamingV4Params, log, callback),
        ], (err, res) => {
            if (err) {
                return callback(err, null, corsHeaders);
            }
            const newByteLength = request.parsedContentLength;
            const oldByteLength = objMD ? objMD['content-length'] : null;
            pushMetric('putObject', log, { authInfo, bucket: bucketName,
                newByteLength, oldByteLength });
            if (res) {
                corsHeaders.ETag = `"${res.contentMD5}"`;
            }
            const vcfg = bucket.getVersioningConfiguration();
            if (vcfg && vcfg.Status === 'Enabled') {
                if (res && res.versionId) {
                    corsHeaders['x-amz-version-id'] =
                        VID.encrypt(res.versionId);
                }
            }
            return callback(null, res, corsHeaders);
        });
    });
}

module.exports = { createAndStoreObject, objectPut };
