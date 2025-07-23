const async = require('async');
const { errors } = require('arsenal');

const metadata = require('./wrapper');
const BucketInfo = require('arsenal').models.BucketInfo;
const { isBucketAuthorized, isObjAuthorized } =
    require('../api/apiUtils/authorization/permissionChecks');
const bucketShield = require('../api/apiUtils/bucket/bucketShield');
const { onlyOwnerAllowed } = require('../../constants');
const { actionNeedQuotaCheck, actionWithDataDeletion } = require('arsenal/build/lib/policyEvaluator/RequestContext');
const { processBytesToWrite, validateQuotas } = require('../api/apiUtils/quotas/quotaUtils');

/** getNullVersionFromMaster - retrieves the null version
 * metadata via retrieving the master key
 *
 * Used in the following cases:
 *
 * - master key is non-versioned (and hence is the 'null' version)
 *
 * - the null version is stored in a versioned key and its reference
 *   is in the master key as 'nullVersionId' (compatibility mode with
 *   old null version storage)
 *
 * @param {string} bucketName - name of bucket
 * @param {string} objectKey - name of object key
 * @param {RequestLogger} log - request logger
 * @param {function} cb - callback(err: Error, nullMD: object)
 * @return {undefined}
 */
function getNullVersionFromMaster(bucketName, objectKey, log, cb) {
    async.waterfall([
        next => metadata.getObjectMD(bucketName, objectKey, {}, log, next),
        (masterMD, next) => {
            if (masterMD.isNull || !masterMD.versionId) {
                log.debug('null version is master version');
                return process.nextTick(() => next(null, masterMD));
            }
            if (masterMD.nullVersionId) {
                // the latest version is not the null version, but null version exists
                // NOTE: for backward-compat with old null version scheme
                log.debug('get the null version via nullVersionId');
                const getOptions = {
                    versionId: masterMD.nullVersionId,
                };
                return metadata.getObjectMD(bucketName, objectKey, getOptions, log, next);
            }
            return next(errors.NoSuchKey);
        },
    ], (err, nullMD) => {
        if (err && err.is && err.is.NoSuchKey) {
            log.debug('could not find a null version');
            return cb();
        }
        if (err) {
            log.debug('err getting object MD from metadata', { error: err });
            return cb(err);
        }
        return cb(null, nullMD);
    });
}

/** metadataGetObject - retrieves specified object or version from metadata
 * @param {string} bucketName - name of bucket
 * @param {string} objectKey - name of object key
 * @param {string} [versionId] - version of object to retrieve
 * @param {object} cachedDocuments - cached version of the documents used for
 *                                   abstraction purposes
 * @param {RequestLogger} log - request logger
 * @param {function} cb - callback
 * @return {undefined} - and call callback with err, bucket md and object md
 */
function metadataGetObject(bucketName, objectKey, versionId, cachedDocuments, log, cb) {
    // versionId may be 'null', which asks metadata to fetch the null key specifically
    const options = { versionId, getDeleteMarker: true };
    if (cachedDocuments && cachedDocuments[objectKey]) {
        return cb(null, cachedDocuments[objectKey]);
    }
    return metadata.getObjectMD(bucketName, objectKey, options, log,
        (err, objMD) => {
            if (err) {
                if (err.is && err.is.NoSuchKey && versionId === 'null') {
                    return getNullVersionFromMaster(bucketName, objectKey, log, cb);
                }
                if (err.is && err.is.NoSuchKey) {
                    log.debug('object does not exist in metadata');
                    return cb();
                }
                log.debug('err getting object MD from metadata', { error: err });
                return cb(err);
            }
            return cb(null, objMD);
        });
}

/** metadataGetObjects - retrieves specified object or version from metadata. This
 * method uses cursors, hence is only compatible with a MongoDB DB backend.
 * @param {string} bucketName - name of bucket
 * @param {string} objectsKeys - name of object key
 * @param {RequestLogger} log - request logger
 * @param {function} cb - callback
 * @return {undefined} - and call callback with err, bucket md and object md
 */
function metadataGetObjects(bucketName, objectsKeys, log, cb) {
    const options = { getDeleteMarker: true };
    const objects = objectsKeys.map(objectKey => ({
        key: objectKey ? objectKey.inPlay.key : null,
        params: options,
        versionId: objectKey ? objectKey.versionId : null,
    }));

    // Returned objects are following the following format: { key, doc, versionId }
    // That is required with batching to properly map the objects
    return metadata.getObjectsMD(bucketName, objects, log, (err, objMds) => {
        if (err) {
            log.debug('error getting objects MD from metadata', { error: err });
            return cb(err);
        }

        const result = {};
        objMds.forEach(objMd => {
            if (objMd.doc) {
                result[`${objMd.doc.key}${objMd.versionId}`] = objMd.doc;
            }
        });

        return cb(null, result);
    });
}
/**
 * Validate that a bucket is accessible and authorized to the user,
 * return a specific error code otherwise
 *
 * @param {BucketInfo} bucket - bucket info
 * @param {object} params - function parameters
 * @param {AuthInfo} params.authInfo - AuthInfo class instance, requester's info
 * @param {string} params.requestType - type of request
 * @param {string} [params.preciseRequestType] - precise type of request
 * @param {object} params.request - http request object
 *  @param {RequestLogger} log - request logger
 * @param {object} actionImplicitDenies - identity authorization results
 * @return {ArsenalError|null} returns a validation error, or null if validation OK
 * The following errors may be returned:
 * - NoSuchBucket: bucket is shielded
 * - MethodNotAllowed: requester is not bucket owner and asking for a
 *     bucket policy operation
 * - AccessDenied: bucket is not authorized
 */
function validateBucket(bucket, params, log, actionImplicitDenies = {}) {
    const { authInfo, preciseRequestType, request } = params;
    let requestType = params.requestType;
    if (bucketShield(bucket, requestType)) {
        log.debug('bucket is shielded from request', {
            requestType,
            method: 'validateBucket',
        });
        return errors.NoSuchBucket;
    }
    const canonicalID = authInfo.getCanonicalID();
    if (!Array.isArray(requestType)) {
        requestType = [requestType];
    }
    if (bucket.getOwner() !== canonicalID && requestType.some(type => onlyOwnerAllowed.includes(type))) {
        return errors.MethodNotAllowed;
    }
    if (!isBucketAuthorized(bucket, (preciseRequestType || requestType), canonicalID,
        authInfo, log, request, actionImplicitDenies)) {
        log.debug('access denied for user on bucket', { requestType });
        return errors.AccessDenied;
    }
    return null;
}
/** standardMetadataValidateBucketAndObj - retrieve bucket and object md from metadata
 * and check if user is authorized to access them.
 * @param {object} params - function parameters
 * @param {AuthInfo} params.authInfo - AuthInfo class instance, requester's info
 * @param {string} params.bucketName - name of bucket
 * @param {string} params.objectKey - name of object
 * @param {string} [params.versionId] - version id if getting specific version
 * @param {string} params.requestType - type of request
 * @param {object} params.request - http request object
 * @param {boolean} actionImplicitDenies - identity authorization results
 * @param {RequestLogger} log - request logger
 * @param {function} callback - callback
 * @return {undefined} - and call callback with params err, bucket md
 */
function standardMetadataValidateBucketAndObj(params, actionImplicitDenies, log, callback) {
    const { authInfo, bucketName, objectKey, versionId, getDeleteMarker, request, withVersionId } = params;
    let requestType = params.requestType;
    if (!Array.isArray(requestType)) {
        requestType = [requestType];
    }
    async.waterfall([
        next => {
            // versionId may be 'null', which asks metadata to fetch the null key specifically
            const getOptions = { versionId };
            if (getDeleteMarker) {
                getOptions.getDeleteMarker = true;
            }
            return metadata.getBucketAndObjectMD(bucketName, objectKey, getOptions, log, (err, getResult) => {
                if (err) {
                    // if some implicit iamAuthzResults, return AccessDenied
                    // before leaking any state information
                    if (actionImplicitDenies && Object.values(actionImplicitDenies).some(v => v === true)) {
                        return next(errors.AccessDenied);
                    }
                    return next(err);
                }
                return next(null, getResult);
            });
        },
        (getResult, next) => {
            const bucket = getResult.bucket ?
                  BucketInfo.deSerialize(getResult.bucket) : undefined;
            if (!bucket) {
                log.debug('bucketAttrs is undefined', {
                    bucket: bucketName,
                    method: 'metadataValidateBucketAndObj',
                });
                return next(errors.NoSuchBucket);
            }
            const validationError = validateBucket(bucket, params, log, actionImplicitDenies);
            if (validationError) {
                return next(validationError, bucket);
            }
            const objMD = getResult.obj ? JSON.parse(getResult.obj) : undefined;
            if (!objMD && versionId === 'null') {
                return getNullVersionFromMaster(bucketName, objectKey, log,
                     (err, nullVer) => next(err, bucket, nullVer));
            }
            return next(null, bucket, objMD);
        },
        (bucket, objMD, next) => {
            const canonicalID = authInfo.getCanonicalID();
            if (!isObjAuthorized(bucket, objMD, requestType, canonicalID, authInfo, log, request,
                actionImplicitDenies)) {
                log.debug('access denied for user on object', { requestType });
                return next(errors.AccessDenied, bucket);
            }
            return next(null, bucket, objMD);
        },
        (bucket, objMD, next) => {
            const needQuotaCheck = requestType => requestType.some(type => actionNeedQuotaCheck[type] ||
                actionWithDataDeletion[type]);
            const checkQuota = params.checkQuota === undefined ? needQuotaCheck(requestType) : params.checkQuota;
            // withVersionId cover cases when an object is being restored with a specific version ID.
            // In this case, the storage space was already accounted for when the RestoreObject API call
            // was made, so we don't need to add any inflight, but quota must be evaluated.
            if (!checkQuota) {
                return next(null, bucket, objMD);
            }
            const contentLength = processBytesToWrite(request.apiMethod, bucket, versionId,
                request?.parsedContentLength || 0, objMD, params.destObjMD);
            return validateQuotas(request, bucket, request.accountQuotas, requestType, request.apiMethod,
                contentLength, withVersionId, log, err => next(err, bucket, objMD));
        },
    ], (err, bucket, objMD) => {
        if (err) {
            // still return bucket for cors headers
            return callback(err, bucket);
        }
        return callback(null, bucket, objMD);
    });
}
/** standardMetadataValidateBucket - retrieve bucket from metadata and check if user
 * is authorized to access it
 * @param {object} params - function parameters
 * @param {AuthInfo} params.authInfo - AuthInfo class instance, requester's info
 * @param {string} params.bucketName - name of bucket
 * @param {string} params.requestType - type of request
 * @param {string} params.request - http request object
 * @param {boolean} actionImplicitDenies - identity authorization results
 * @param {RequestLogger} log - request logger
 * @param {function} callback - callback
 * @return {undefined} - and call callback with params err, bucket md
 */
function standardMetadataValidateBucket(params, actionImplicitDenies, log, callback) {
    const { bucketName } = params;
    return metadata.getBucket(bucketName, log, (err, bucket) => {
        if (err) {
            // if some implicit actionImplicitDenies, return AccessDenied before
            // leaking any state information
            if (actionImplicitDenies && Object.values(actionImplicitDenies).some(v => v === true)) {
                return callback(errors.AccessDenied);
            }
            log.debug('metadata getbucket failed', { error: err });
            return callback(err);
        }
        const validationError = validateBucket(bucket, params, log, actionImplicitDenies);
        return callback(validationError, bucket);
    });
}

module.exports = {
    validateBucket,
    metadataGetObject,
    metadataGetObjects,
    processBytesToWrite,
    standardMetadataValidateBucketAndObj,
    standardMetadataValidateBucket,
};
