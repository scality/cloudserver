const { promisify } = require('util');
const async = require('async');
const { errors } = require('arsenal');

const metadata = require('./wrapper');
const BucketInfo = require('arsenal').models.BucketInfo;
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const { isBucketAuthorized, isObjAuthorized } =
    require('../api/apiUtils/authorization/permissionChecks');
const { isRateLimitServiceUser } = require('../api/apiUtils/authorization/serviceUser');
const bucketShield = require('../api/apiUtils/bucket/bucketShield');
const { onlyOwnerAllowed } = require('../../constants');
const { actionNeedQuotaCheck, actionWithDataDeletion } = require('arsenal/build/lib/policyEvaluator/RequestContext');
const { processBytesToWrite, validateQuotas } = require('../api/apiUtils/quotas/quotaUtils');
const { config } = require('../Config');
const cache = require('../api/apiUtils/rateLimit/cache');
const {
    rateLimitApiActions,
    requestNeedsRateCheck,
    extractRateLimitConfigFromRequest,
    buildRateChecksFromConfig,
    checkRateLimitsForRequest,
} = require('../api/apiUtils/rateLimit/helpers');

/**
 * Store server access log information in the request object
 * @param {object} request - HTTP request object
 * @param {object} bucket - Bucket metadata object
 * @param {string} raftSessionId - Raft session ID from metadata backend
 * @param {object} [options] - Optional configuration
 * @param {boolean} [options.copySource=false] - If true, stores information in request.sourceServerAccessLog
 *   instead of request.serverAccessLog. Used for copy operations to preserve both source and destination
 *   bucket information separately. When false (default), stores in request.serverAccessLog.
 * @return {undefined}
 */
function storeServerAccessLogInfo(request, bucket, raftSessionId, options = {}) {
    /* eslint-disable no-param-reassign */

    if (!request || !request.serverAccessLog) {
        return;
    }

    if (options.copySource) {
        // Copy operations need information for a special GET log entry on the source side
        if (!request.sourceServerAccessLog) {
            request.sourceServerAccessLog = { enabled: false };
        }
        const target = request.sourceServerAccessLog;
        target.raftSessionID = raftSessionId;
        if (bucket) {
            target.bucketOwner = bucket.getOwner();
            target.bucketName = bucket.getName();
        }
        if (bucket && bucket.getBucketLoggingStatus() && bucket.getBucketLoggingStatus().getLoggingEnabled()) {
            target.enabled = true;
            target.loggingEnabled = bucket.getBucketLoggingStatus().getLoggingEnabled();
        }
        // Source auth runs on the same request object and overwrites the
        // destination's aclRequired. Move the source value to its log entry
        // and restore the destination value that was saved before source auth.
        target.aclRequired = request.serverAccessLog.aclRequired;
        request.serverAccessLog.aclRequired = options.savedAclRequired;
    } else {
        // Default behavior: store in main serverAccessLog
        request.serverAccessLog.raftSessionID = raftSessionId;
        if (bucket) {
            request.serverAccessLog.bucketOwner = bucket.getOwner();
        }
        if (bucket && bucket.getBucketLoggingStatus() && bucket.getBucketLoggingStatus().getLoggingEnabled()) {
            request.serverAccessLog.enabled = true;
            request.serverAccessLog.loggingEnabled = bucket.getBucketLoggingStatus().getLoggingEnabled();
        }
    }

    /* eslint-enable no-param-reassign */
}

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

    // Skip checking bucket ownership if the requesting user is the rate limit service user
    // and the requestType is Get/Put/DeleteBucketRateLimit.
    if (requestType.every(type => rateLimitApiActions.includes(type))
        && config.rateLimiting.enabled
        && isRateLimitServiceUser(authInfo)
    ) {
        return null;
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

/**
 * Check rate limiting if not already checked
 *
 * Extracts rate limit config from bucket metadata, caches it, and enforces limit.
 * Calls callback with error if rate limited, null if allowed or no rate limiting.
 *
 * @param {object} request - Request object
 * @param {AuthInfo} authInfo - AuthInfo class instance, requester's info
 * @param {BucketInfo} bucketMD - Bucket metadata
 * @param {object} log - Logger instance
 * @param {function} callback - Callback(err) - err if rate limited, null if allowed
 * @returns {undefined}
 */
function checkRateLimitIfNeeded(request, authInfo, bucketMD, log, callback) {
    // Skip if already checked or not enabled
    if (!requestNeedsRateCheck(request)) {
        return process.nextTick(callback, null);
    }

    // Extract rate limit config from bucket metadata and cache it
    const checks = [];
    const rateLimitConfig = extractRateLimitConfigFromRequest(request, authInfo, bucketMD, log);

    cache.setCachedBucketOwner(bucketMD.getName(), bucketMD.getOwner(), config.rateLimiting.bucket.configCacheTTL);

    if (!request.rateLimitBucketAlreadyChecked && rateLimitConfig.bucket !== undefined) {
        cache.setCachedConfig(
            cache.namespace.bucket,
            bucketMD.getName(),
            rateLimitConfig.bucket,
            config.rateLimiting.bucket.configCacheTTL
        );
        checks.push(...buildRateChecksFromConfig('bucket', bucketMD.getName(), rateLimitConfig.bucket));
        // eslint-disable-next-line no-param-reassign
        request.rateLimitBucketAlreadyChecked = true;
    }

    if (
        !request.rateLimitAccountAlreadyChecked
        && rateLimitConfig.account !== undefined
        && !(authInfo.isRequesterPublicUser && authInfo.isRequesterPublicUser())
    ) {
        const targetAccount = request.rateLimitTargetAccount
            ? request.rateLimitTargetAccount
            : authInfo.getCanonicalID();

        cache.setCachedConfig(
            cache.namespace.account,
            targetAccount,
            rateLimitConfig.account,
            config.rateLimiting.account.configCacheTTL
        );
        checks.push(...buildRateChecksFromConfig('account', targetAccount, rateLimitConfig.account));
        // eslint-disable-next-line no-param-reassign
        request.rateLimitAccountAlreadyChecked = true;
    }

    const { allowed, rateLimitSource } = checkRateLimitsForRequest(checks, log);
    if (!allowed) {
        log.addDefaultFields({
            rateLimited: true,
            rateLimitSource,
        });

        if (request.serverAccessLog) {
            /* eslint-disable no-param-reassign */
            request.serverAccessLog.rateLimited = true;
            request.serverAccessLog.rateLimitSource = rateLimitSource;
            /* eslint-enable no-param-reassign */
        }

        return process.nextTick(callback, config.rateLimiting.error);
    }

    return process.nextTick(callback, null);
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
 * @param {object} [params.serverAccessLogOptions] - options for server access logging
 * @param {boolean} [params.serverAccessLogOptions.copySource] - if true, stores source bucket
 *   information in request.sourceServerAccessLog for copy operations
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
    if (params.serverAccessLogOptions?.copySource) {
        // eslint-disable-next-line no-param-reassign
        params.serverAccessLogOptions.savedAclRequired = request?.serverAccessLog?.aclRequired;
    }
    async.waterfall([
        next => {
            // versionId may be 'null', which asks metadata to fetch the null key specifically
            const getOptions = { versionId };
            if (getDeleteMarker) {
                getOptions.getDeleteMarker = true;
            }
            return metadata.getBucketAndObjectMD(bucketName, objectKey, getOptions, log,
                (err, getResult, raftSessionId) => {
                    if (err) {
                        // if some implicit iamAuthzResults, return AccessDenied
                        // before leaking any state information
                        if (actionImplicitDenies && Object.values(actionImplicitDenies).some(v => v === true)) {
                            return next(errors.AccessDenied);
                        }
                        return next(err);
                    }
                    return next(null, getResult, raftSessionId);
                });
        },
        (getResult, raftSessionId, next) => {
            const bucket = getResult.bucket ?
                  BucketInfo.deSerialize(getResult.bucket) : undefined;
            if (!bucket) {
                log.debug('bucketAttrs is undefined', {
                    bucket: bucketName,
                    method: 'metadataValidateBucketAndObj',
                });
                return next(errors.NoSuchBucket, raftSessionId);
            }
            const validationError = validateBucket(bucket, params, log, actionImplicitDenies);
            if (validationError) {
                return next(validationError, bucket, raftSessionId);
            }

            // Rate limiting check if not already done in api.js
            return checkRateLimitIfNeeded(request, authInfo, bucket, log, err => {
                if (err) {
                    return next(err, bucket);
                }

                // Continue with object metadata processing
                const objMD = getResult.obj ? JSON.parse(getResult.obj) : undefined;
                if (!objMD && versionId === 'null') {
                    return getNullVersionFromMaster(bucketName, objectKey, log,
                         (err, nullVer) => next(err, bucket, nullVer, raftSessionId));
                }
                return next(null, bucket, objMD, raftSessionId);
            });
        },
        (bucket, objMD, raftSessionId, next) => {
            const objMetadata = objMD;
            const canonicalID = authInfo.getCanonicalID();
            if (!isObjAuthorized(bucket, objMetadata, requestType, canonicalID, authInfo, log, request,
                actionImplicitDenies)) {
                log.debug('access denied for user on object', { requestType });
                return next(errors.AccessDenied, bucket, undefined, raftSessionId);
            }

            if (!objMetadata) {
                return next(null, bucket, objMetadata, raftSessionId);
            }

            let returnTagCount = false;
            if (params.returnTagCount) {
                // If returnTagCount is true we know that Vault authorized the request so it is not an implicitDeny.
                const implicitDeny = false;
                if (requestType.some(r => r === 'objectGet')) {
                    returnTagCount = isObjAuthorized(bucket, objMetadata, ['objectGetTagging'], canonicalID, authInfo,
                        log, request, implicitDeny);
                } else if (requestType.some(r => r === 'objectGetVersion')) {
                    returnTagCount = isObjAuthorized(bucket, objMetadata, ['objectGetTaggingVersion'],
                        canonicalID, authInfo, log, request, implicitDeny);
                }

                objMetadata.returnTagCount = returnTagCount;
            }
            return next(null, bucket, objMetadata, raftSessionId);
        },
        (bucket, objMD, raftSessionId, next) => {
            const needQuotaCheck = requestType => requestType.some(type => actionNeedQuotaCheck[type] ||
                actionWithDataDeletion[type]);
            const checkQuota = params.checkQuota === undefined ? needQuotaCheck(requestType) : params.checkQuota;
            // withVersionId cover cases when an object is being restored with a specific version ID.
            // In this case, the storage space was already accounted for when the RestoreObject API call
            // was made, so we don't need to add any inflight, but quota must be evaluated.
            if (!checkQuota) {
                return next(null, bucket, objMD, raftSessionId);
            }
            const contentLength = processBytesToWrite(request.apiMethod, bucket, versionId,
                request?.parsedContentLength || 0, objMD, params.destObjMD);
            return validateQuotas(request, bucket, request.accountQuotas, requestType, request.apiMethod,
                contentLength, withVersionId, log, err => next(err, bucket, objMD, raftSessionId));
        },
    ], (err, bucket, objMD, raftSessionId) => {
        storeServerAccessLogInfo(request, bucket, raftSessionId, params.serverAccessLogOptions);
        if (err) {
            // still return bucket for cors headers
            return callback(err, bucket);
        }
        return callback(null, bucket, objMD);
    });
}
// When the validate function returns (err, bucket) - which it does even on
// post-load failures like ACL denials - we want the bucket to flow through
// to the catch-side of async/await callers so they can set CORS headers on
// 403 responses without a second metadata.getBucket. Default `promisify`
// drops everything past `err` on rejection; these custom resolvers attach
// the computed corsHeaders to the error instead. The wrapper in
// lib/api/api.js (wrapCallbackWithErrorCorsHeaders) reads
// err.additionalResHeaders on the fast path.
function attachCorsHeadersToError(err, bucket, params) {
    const request = params && params.request;
    if (!err || !bucket || !request || !request.headers
        || !request.headers.origin) {
        return err;
    }
    // eslint-disable-next-line no-param-reassign
    err.additionalResHeaders = collectCorsHeaders(
        request.headers.origin, request.method, bucket);
    return err;
}

standardMetadataValidateBucketAndObj[promisify.custom] = (params, ...rest) =>
    new Promise((resolve, reject) => {
        standardMetadataValidateBucketAndObj(params, ...rest, (err, bucket, objectMD) => {
            if (err) {
                return reject(attachCorsHeadersToError(err, bucket, params));
            }
            return resolve({ bucket, objectMD });
        });
    });

standardMetadataValidateBucket[promisify.custom] = (params, ...rest) =>
    new Promise((resolve, reject) => {
        standardMetadataValidateBucket(params, ...rest, (err, bucket) => {
            if (err) {
                return reject(attachCorsHeadersToError(err, bucket, params));
            }
            return resolve(bucket);
        });
    });

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
    const { bucketName, request } = params;
    return metadata.getBucket(bucketName, log, (err, bucket, raftSessionId) => {
        storeServerAccessLogInfo(params.request, bucket, raftSessionId);
        if (err) {
            // if some implicit actionImplicitDenies, return AccessDenied before
            // leaking any state information
            if (actionImplicitDenies && Object.values(actionImplicitDenies).some(v => v === true)) {
                return callback(errors.AccessDenied);
            }
            log.debug('metadata getbucket failed', { error: err });
            return callback(err);
        }

        // Rate limiting check if not already done in api.js
        return checkRateLimitIfNeeded(request, params.authInfo, bucket, log, err => {
            if (err) {
                return callback(err);
            }

            // Continue with validation
            const validationError = validateBucket(bucket, params, log, actionImplicitDenies);
            return callback(validationError, bucket);
        });
    });
}

module.exports = {
    validateBucket,
    metadataGetObject,
    metadataGetObjects,
    processBytesToWrite,
    standardMetadataValidateBucketAndObj,
    standardMetadataValidateBucket,
    storeServerAccessLogInfo,
};
