const async = require('async');
const { errors } = require('arsenal');

const metadata = require('./wrapper');
const BucketInfo = require('arsenal').models.BucketInfo;
const { isBucketAuthorized, isObjAuthorized } =
    require('../api/apiUtils/authorization/permissionChecks');
const bucketShield = require('../api/apiUtils/bucket/bucketShield');

/** _parseListEntries - parse the values returned in a listing by metadata
 * @param {object[]} entries - Version or Content entries in a metadata listing
 * @param {string} entries[].key - metadata key
 * @param {string} entries[].value - stringified object metadata
 * @return {object} - mapped array with parsed value or JSON parsing err
 */
function _parseListEntries(entries) {
    return entries.map(entry => {
        if (typeof entry.value === 'string') {
            const tmp = JSON.parse(entry.value);
            return {
                key: entry.key,
                value: {
                    Size: tmp['content-length'],
                    ETag: tmp['content-md5'],
                    VersionId: tmp.versionId,
                    IsNull: tmp.isNull,
                    IsDeleteMarker: tmp.isDeleteMarker,
                    LastModified: tmp['last-modified'],
                    Owner: {
                        DisplayName: tmp['owner-display-name'],
                        ID: tmp['owner-id'],
                    },
                    StorageClass: tmp['x-amz-storage-class'],
                    // MPU listing properties
                    Initiated: tmp.initiated,
                    Initiator: tmp.initiator,
                    EventualStorageBucket: tmp.eventualStorageBucket,
                    partLocations: tmp.partLocations,
                    creationDate: tmp.creationDate,
                },
            };
        }
        return entry;
    });
}

/** parseListEntries - parse the values returned in a listing by metadata
 * @param {object[]} entries - Version or Content entries in a metadata listing
 * @param {string} entries[].key - metadata key
 * @param {string} entries[].value - stringified object metadata
 * @return {(object|Error)} - mapped array with parsed value or JSON parsing err
 */
function parseListEntries(entries) {
    // wrap private function in a try/catch clause
    // just in case JSON parsing throws an exception
    try {
        return _parseListEntries(entries);
    } catch (e) {
        return e;
    }
}

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
        return metadata.getObjectMD(bucketName, objectKey, options, log, cb);
    }
    log.debug('could not find a null version');
    return process.nextTick(() => cb());
}

/** metadataGetBucketAndObject - retrieves bucket and specified version
 * NOTE: If the value of `versionId` param is 'null', this function returns the
 * master version objMD. The null version object md must be retrieved in a
 * separate step using the master object md: see getNullVersion().
 * @param {string} requestType - type of request
 * @param {string} bucketName - name of bucket
 * @param {string} objectKey - name of object key
 * @param {string} [versionId] - version of object to retrieve
 * @param {RequestLogger} log - request logger
 * @param {function} cb - callback
 * @return {undefined} - and call callback with err, bucket md and object md
 */
function metadataGetBucketAndObject(requestType, bucketName, objectKey,
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
            if (bucketShield(bucket, requestType)) {
                log.debug('bucket is shielded from request', {
                    requestType,
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

/** metadataValidateBucketAndObj - retrieve bucket and object md from metadata
 * and check if user is authorized to access them.
 * @param {object} params - function parameters
 * @param {AuthInfo} params.authInfo - AuthInfo class instance, requester's info
 * @param {string} params.bucketName - name of bucket
 * @param {string} params.objectKey - name of object
 * @param {string} [params.versionId] - version id if getting specific version
 * @param {string} params.requestType - type of request
 * @param {object} params.request - http request object
 * @param {RequestLogger} log - request logger
 * @param {function} callback - callback
 * @return {undefined} - and call callback with params err, bucket md
 */
function metadataValidateBucketAndObj(params, log, callback) {
    const { authInfo, bucketName, objectKey, versionId, requestType, preciseRequestType, request } = params;
    const canonicalID = authInfo.getCanonicalID();
    async.waterfall([
        function getBucketAndObjectMD(next) {
            return metadataGetBucketAndObject(requestType, bucketName,
                objectKey, versionId, log, next);
        },
        function checkBucketAuth(bucket, objMD, next) {
            // if requester is not bucket owner, bucket policy actions should be denied with
            // MethodNotAllowed error
            const onlyOwnerAllowed = ['bucketDeletePolicy', 'bucketGetPolicy', 'bucketPutPolicy'];
            if (bucket.getOwner() !== canonicalID && onlyOwnerAllowed.includes(requestType)) {
                return next(errors.MethodNotAllowed, bucket);
            }
            if (!isBucketAuthorized(bucket, (preciseRequestType || requestType), canonicalID,
            authInfo, log, request)) {
                log.debug('access denied for user on bucket', { requestType });
                return next(errors.AccessDenied, bucket);
            }
            return next(null, bucket, objMD);
        },
        function handleNullVersionGet(bucket, objMD, next) {
            if (objMD && versionId === 'null') {
                return getNullVersion(objMD, bucketName, objectKey, log,
                    (err, nullVer) => next(err, bucket, nullVer));
            }
            return next(null, bucket, objMD);
        },
        function checkObjectAuth(bucket, objMD, next) {
            if (!isObjAuthorized(bucket, objMD, requestType, canonicalID, authInfo, log, request)) {
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
 * @param {RequestLogger} log - request logger
 * @param {function} callback - callback
 * @return {undefined} - and call callback with params err, bucket md
 */
function metadataValidateBucket(params, log, callback) {
    const { authInfo, bucketName, requestType, preciseRequestType, request } = params;
    const canonicalID = authInfo.getCanonicalID();
    return metadataGetBucket(requestType, bucketName, log, (err, bucket) => {
        if (err) {
            return callback(err);
        }
        // if requester is not bucket owner, bucket policy actions should be denied with
        // MethodNotAllowed error
        const onlyOwnerAllowed = ['bucketDeletePolicy', 'bucketGetPolicy', 'bucketPutPolicy'];
        if (bucket.getOwner() !== canonicalID && onlyOwnerAllowed.includes(requestType)) {
            return callback(errors.MethodNotAllowed, bucket);
        }
        // still return bucket for cors headers
        if (!isBucketAuthorized(bucket, (preciseRequestType || requestType), canonicalID, authInfo, log, request)) {
            log.debug('access denied for user on bucket', { requestType });
            return callback(errors.AccessDenied, bucket);
        }
        return callback(null, bucket);
    });
}

module.exports = {
    parseListEntries,
    metadataGetObject,
    metadataValidateBucketAndObj,
    metadataValidateBucket,
};
