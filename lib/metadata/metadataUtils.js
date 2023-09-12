const async = require('async');
const { errors } = require('arsenal');

const metadata = require('./wrapper');
const BucketInfo = require('arsenal').models.BucketInfo;
const { isBucketAuthorized, isObjAuthorized } =
    require('../api/apiUtils/authorization/permissionChecks');
const bucketShield = require('../api/apiUtils/bucket/bucketShield');

/** getNullVersion - return metadata of null version if it exists
 * @param {object} objMD - metadata of master version
 * @param {string} bucketName - name of bucket
 * @param {string} objectKey - name of object key
 * @param {RequestLogger} log - request logger
 * @param {function} cb - callback
 * @return {undefined} - and call callback with params err, objMD of null ver
 */
function getNullVersion(objMD, bucketName, objectKey, log, cb) {
    const options = {};
    if (objMD.isNull || !objMD.versionId) {
        // null version is current version
        log.debug('found null version');
        return process.nextTick(() => cb(null, objMD));
    }
    if (objMD.nullVersionId) {
        // the latest version is not the null version, but null version exists
        log.debug('null version exists, get the null version');
        options.versionId = objMD.nullVersionId;
        return metadata.getObjectMD(bucketName, objectKey, options, log, (err, nullVersionMD) => {
            if (err && err.is && err.is.NoSuchKey) {
                log.debug('object does not exist in metadata');
                return cb();
            }
            return cb(err, nullVersionMD);
        });
    }
    log.debug('could not find a null version');
    return process.nextTick(() => cb());
}

/** metadataGetBucketAndObject - retrieves bucket and specified version
 * NOTE: If the value of `versionId` param is 'null', this function returns the
 * master version objMD. The null version object md must be retrieved in a
 * separate step using the master object md: see getNullVersion().
 * @param {string} bucketName - name of bucket
 * @param {string} objectKey - name of object key
 * @param {string} [versionId] - version of object to retrieve
 * @param {RequestLogger} log - request logger
 * @param {function} cb - callback
 * @return {undefined} - and call callback with err, bucket md and object md
 */
function metadataGetBucketAndObject(bucketName, objectKey,
    versionId, log, cb) {
    const options = {
        // if attempting to get 'null' version, must retrieve null version id
        // from most current object md (versionId = undefined)
        versionId: versionId === 'null' ? undefined : versionId,
    };
    return metadata.getBucketAndObjectMD(bucketName, objectKey, options, log,
        (err, data) => {
            if (err) {
                log.debug('metadata get failed', { error: err });
                return cb(err);
            }
            const bucket = data.bucket ? BucketInfo.deSerialize(data.bucket) :
                undefined;
            const obj = data.obj ? JSON.parse(data.obj) : undefined;
            if (!bucket) {
                log.debug('bucketAttrs is undefined', {
                    bucket: bucketName,
                    method: 'metadataGetBucketAndObject',
                });
                return cb(errors.NoSuchBucket);
            }
            log.trace('found bucket in metadata');
            return cb(null, bucket, obj);
        });
}

/** metadataGetObject - retrieves specified object or version from metadata
 * @param {string} bucketName - name of bucket
 * @param {string} objectKey - name of object key
 * @param {string} [versionId] - version of object to retrieve
 * @param {RequestLogger} log - request logger
 * @param {function} cb - callback
 * @return {undefined} - and call callback with err, bucket md and object md
 */
function metadataGetObject(bucketName, objectKey, versionId, log, cb) {
    const options = {
        // if attempting to get 'null' version, must first retrieve null version
        // id from most current object md (by setting versionId as undefined
        // we retrieve the most current object md)
        versionId: versionId === 'null' ? undefined : versionId,
    };
    return metadata.getObjectMD(bucketName, objectKey, options, log,
        (err, objMD) => {
            if (err && err.is && err.is.NoSuchKey) {
                log.debug('object does not exist in metadata');
                return cb();
            }
            if (err) {
                log.debug('err getting object MD from metadata',
                { error: err });
                return cb(err);
            }
            if (versionId === 'null') {
                return getNullVersion(objMD, bucketName, objectKey, log, cb);
            }
            return cb(null, objMD);
        });
}

function validateBucket(bucket, params, iamAuthzResults, log) {
    const { authInfo, preciseRequestType, request } = params;
    let requestType = params.requestType;
    if (bucketShield(bucket, requestType)) {
        log.debug('bucket is shielded from request', {
            requestType,
            method: 'validateBucket',
        });
        return errors.NoSuchBucket;
    }
    // if requester is not bucket owner, bucket policy actions should be denied with
    // MethodNotAllowed error
    const onlyOwnerAllowed = ['bucketDeletePolicy', 'bucketGetPolicy', 'bucketPutPolicy'];
    const canonicalID = authInfo.getCanonicalID();
    if (!Array.isArray(requestType)) {
        requestType = [requestType];
    }
    if (bucket.getOwner() !== canonicalID && requestType.some(type => onlyOwnerAllowed.includes(type))) {
        return errors.MethodNotAllowed;
    }
    if (!isBucketAuthorized(bucket, (preciseRequestType || requestType), canonicalID,
                            authInfo, iamAuthzResults, log, request)) {
        log.debug('access denied for user on bucket', { requestType });
        return errors.AccessDenied;
    }
    return null;
}


/** metadataValidateBucketAndObj - retrieve bucket and object md from metadata
 * and check if user is authorized to access them.
 * @param {object} params - function parameters
 * @param {AuthInfo} params.authInfo - AuthInfo class instance, requester's info
 * @param {string} params.bucketName - name of bucket
 * @param {string} params.objectKey - name of object
 * @param {string} [params.versionId] - version id if getting specific version
 * @param {string} params.requestType - type of request
 * @param {object} params.request - http request object
 * @param {boolean} iamAuthzResults - identity authorization results
 * @param {RequestLogger} log - request logger
 * @param {function} callback - callback
 * @return {undefined} - and call callback with params err, bucket md
 */
function metadataValidateBucketAndObj(params, iamAuthzResults, log, callback) {
    const { authInfo, bucketName, objectKey, versionId, request } = params;
    let requestType = params.requestType;
    if (!Array.isArray(requestType)) {
        requestType = [requestType];
    }
    async.waterfall([
        next => {
            return metadataGetBucketAndObject(bucketName,
                objectKey, versionId, log, (err, bucket, objMD) => {
                    if (err) {
                        // if some implicit iamAuthzResults, return AccessDenied
                        // before leaking any state information
                        if (iamAuthzResults && Object.values(iamAuthzResults).some(v => v === true)) {
                            return next(errors.AccessDenied);
                        }
                        return next(err);
                    }
                    return next(null, bucket, objMD);
                });
        },
        (bucket, objMD, next) => {
            const validationError = validateBucket(bucket, params, iamAuthzResults, log);
            if (validationError) {
                return next(validationError, bucket);
            }
            if (objMD && versionId === 'null') {
                return getNullVersion(objMD, bucketName, objectKey, log,
                    (err, nullVer) => next(err, bucket, nullVer));
            }
            return next(null, bucket, objMD);
        },
        (bucket, objMD, next) => {
            const canonicalID = authInfo.getCanonicalID();
            if (!isObjAuthorized(bucket, objMD, requestType, canonicalID, authInfo, iamAuthzResults,
                log, request)) {
                log.debug('access denied for user on object', { requestType });
                return next(errors.AccessDenied, bucket);
            }
            return next(null, bucket, objMD);
        },
    ], (err, bucket, objMD) => {
        if (err) {
            // still return bucket for cors headers
            return callback(err, bucket);
        }
        return callback(null, bucket, objMD);
    });
}

/** metadataGetBucket - retrieves bucket from metadata, returning error if
 * bucket is shielded
 * @param {string} requestType - type of request
 * @param {string} bucketName - name of bucket
 * @param {RequestLogger} log - request logger
 * @param {function} cb - callback
 * @return {undefined} - and call callback with err, bucket md
 */
function metadataGetBucket(requestType, bucketName, log, cb) {
    return metadata.getBucket(bucketName, log, (err, bucket) => {
        if (err) {
            log.debug('metadata getbucket failed', { error: err });
            return cb(err);
        }
        if (bucketShield(bucket, requestType)) {
            log.debug('bucket is shielded from request', {
                requestType,
                method: 'metadataGetBucketAndObject',
            });
            return cb(errors.NoSuchBucket);
        }
        log.trace('found bucket in metadata');
        return cb(null, bucket);
    });
}

/** metadataValidateBucket - retrieve bucket from metadata and check if user
 * is authorized to access it
 * @param {object} params - function parameters
 * @param {AuthInfo} params.authInfo - AuthInfo class instance, requester's info
 * @param {string} params.bucketName - name of bucket
 * @param {string} params.requestType - type of request
 * @param {string} params.request - http request object
 * @param {boolean} iamAuthzResults - identity authorization results
 * @param {RequestLogger} log - request logger
 * @param {function} callback - callback
 * @return {undefined} - and call callback with params err, bucket md
 */
function metadataValidateBucket(params, iamAuthzResults, log, callback) {
    const { bucketName, requestType } = params;
    return metadataGetBucket(requestType, bucketName, log, (err, bucket) => {
        if (err) {
            return callback(err);
        }
        const validationError = validateBucket(bucket, params, iamAuthzResults, log);
        return callback(validationError, bucket);
    });
}

module.exports = {
    metadataGetObject,
    validateBucket,
    metadataValidateBucketAndObj,
    metadataValidateBucket,
};
