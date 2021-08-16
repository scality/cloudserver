const async = require('async');
const { errors, versioning, s3middleware } = require('arsenal');
const checkDateModifiedHeaders = s3middleware.checkDateModifiedHeaders;
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const services = require('../services');
const { pushMetric } = require('../utapi/utilities');
const createAndStoreObject = require('./apiUtils/object/createAndStoreObject');
const { decodeVersionId, preprocessingVersioningDelete }
    = require('./apiUtils/object/versioning');
const { isServiceAccount } = require('./apiUtils/authorization/permissionChecks');
const { metadataValidateBucketAndObj } = require('../metadata/metadataUtils');
const monitoring = require('../utilities/monitoringHandler');
const { isObjectLocked } = require('./apiUtils/object/objectLockHelpers');
const { config } = require('../Config');

const versionIdUtils = versioning.VersionID;
const objectLockedError = new Error('object locked');

/**
 * objectDelete - DELETE an object from a bucket
 * @param {AuthInfo} authInfo - requester's infos
 * @param {object} request - request object given by router,
 *                           includes normalized headers
 * @param {Logger} log - werelogs request instance
 * @param {function} cb - final cb to call with the result and response headers
 * @return {undefined}
 */
function objectDelete(authInfo, request, log, cb) {
    log.debug('processing request', { method: 'objectDelete' });
    if (authInfo.isRequesterPublicUser()) {
        log.debug('operation not available for public user');
        monitoring.promMetrics(
            'DELETE', request.bucketName, 403, 'deleteObject');
        return cb(errors.AccessDenied);
    }
    const bucketName = request.bucketName;
    const objectKey = request.objectKey;

    const decodedVidResult = decodeVersionId(request.query);
    if (decodedVidResult instanceof Error) {
        log.trace('invalid versionId query', {
            versionId: request.query.versionId,
            error: decodedVidResult,
        });
        return cb(decodedVidResult);
    }
    const reqVersionId = decodedVidResult;

    const valParams = {
        authInfo,
        bucketName,
        objectKey,
        versionId: reqVersionId,
        requestType: 'objectDelete',
    };

    const canonicalID = authInfo.getCanonicalID();
    return async.waterfall([
        function validateBucketAndObj(next) {
            return metadataValidateBucketAndObj(valParams, log,
            (err, bucketMD, objMD) => {
                if (err) {
                    return next(err, bucketMD);
                }

                if (!isServiceAccount(canonicalID) &&
                    (objMD && objMD['owner-id'] !== canonicalID)
                    && bucketMD.getOwner() !== canonicalID) {
                    return next(errors.AccessDenied);
                }

                const versioningCfg = bucketMD.getVersioningConfiguration();
                if (!objMD) {
                    if (!versioningCfg) {
                        return next(errors.NoSuchKey, bucketMD);
                    }
                    // AWS does not return an error when trying to delete a
                    // specific version that does not exist. We skip to the end
                    // of the waterfall here.
                    if (reqVersionId) {
                        log.debug('trying to delete specific version ' +
                        ' that does not exist');
                        return next(errors.NoSuchVersion, bucketMD);
                    }
                    // To adhere to AWS behavior, create a delete marker even
                    // if trying to delete an object that does not exist when
                    // versioning has been configured
                    return next(null, bucketMD, objMD);
                }
                // AWS only returns an object lock error if a version id
                // is specified, else continue to create a delete marker
                if (reqVersionId &&
                isObjectLocked(bucketMD, objMD, request.headers)) {
                    log.debug('trying to delete locked object');
                    return next(objectLockedError, bucketMD);
                }
                if (reqVersionId && objMD.location &&
                    Array.isArray(objMD.location) && objMD.location[0]) {
                    // we need this information for data deletes to AWS
                    // eslint-disable-next-line no-param-reassign
                    objMD.location[0].deleteVersion = true;
                }
                if (objMD['content-length'] !== undefined) {
                    log.end().addDefaultFields({
                        bytesDeleted: objMD['content-length'],
                    });
                }
                return next(null, bucketMD, objMD);
            });
        },
        function getVersioningInfo(bucketMD, objectMD, next) {
            return preprocessingVersioningDelete(bucketName,
                bucketMD, objectMD, reqVersionId, log,
                (err, options) => {
                    if (err) {
                        log.error('err processing versioning info',
                        { error: err });
                        return next(err, bucketMD);
                    }
                    return next(null, bucketMD, objectMD, options);
                });
        },
        function validateHeaders(bucketMD, objectMD, options, next) {
            if (objectMD) {
                const lastModified = objectMD['last-modified'];
                const { modifiedSinceRes, unmodifiedSinceRes } =
                    checkDateModifiedHeaders(request.headers, lastModified);
                const err = modifiedSinceRes.error || unmodifiedSinceRes.error;
                if (err) {
                    return process.nextTick(() => next(err, bucketMD));
                }
            }
            return process.nextTick(() =>
                next(null, bucketMD, objectMD, options));
        },
        function deleteOperation(bucketMD, objectMD, delOptions, next) {
            const deleteInfo = {
                removeDeleteMarker: false,
                newDeleteMarker: false,
            };
            if (delOptions && delOptions.deleteData && bucketMD.isNFS() &&
                bucketMD.getReplicationConfiguration()) {
                // If an NFS bucket that has replication configured, we want
                // to put a delete marker on the destination even though the
                // source does not have versioning.
                return createAndStoreObject(bucketName, bucketMD, objectKey,
                    objectMD, authInfo, canonicalID, null, request, true, null,
                    log, err => {
                        if (err) {
                            return next(err);
                        }
                        if (objectMD.isDeleteMarker) {
                            // record that we deleted a delete marker to set
                            // response headers accordingly
                            deleteInfo.removeDeleteMarker = true;
                        }
                        return services.deleteObject(bucketName, objectMD,
                            objectKey, delOptions, log, (err, delResult) =>
                            next(err, bucketMD, objectMD, delResult,
                                deleteInfo));
                    });
            }
            if (delOptions && delOptions.deleteData) {
                if (objectMD.isDeleteMarker) {
                    // record that we deleted a delete marker to set
                    // response headers accordingly
                    deleteInfo.removeDeleteMarker = true;
                }
                return services.deleteObject(bucketName, objectMD, objectKey,
                    delOptions, log, (err, delResult) => next(err, bucketMD,
                    objectMD, delResult, deleteInfo));
            }
            // putting a new delete marker
            deleteInfo.newDeleteMarker = true;
            return createAndStoreObject(bucketName, bucketMD,
                objectKey, objectMD, authInfo, canonicalID, null, request,
                deleteInfo.newDeleteMarker, null, log, (err, newDelMarkerRes) =>
                next(err, bucketMD, objectMD, newDelMarkerRes, deleteInfo));
        },
    ], (err, bucketMD, objectMD, result, deleteInfo) => {
        const resHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucketMD);
        // if deleting a specific version or delete marker, return version id
        // in the response headers, even in case of NoSuchVersion
        if (reqVersionId) {
            resHeaders['x-amz-version-id'] = reqVersionId === 'null' ?
                reqVersionId : versionIdUtils.encode(
                    reqVersionId, config.versionIdEncodingType);
            if (deleteInfo && deleteInfo.removeDeleteMarker) {
                resHeaders['x-amz-delete-marker'] = true;
            }
        }
        if (err === objectLockedError) {
            log.debug('preventing deletion due to object lock',
                {
                    error: errors.AccessDenied,
                    objectLocked: true,
                    method: 'objectDelete',
                });
            return cb(errors.AccessDenied, resHeaders);
        }
        if (err) {
            log.debug('error processing request', { error: err,
                method: 'objectDelete' });
            monitoring.promMetrics(
                'DELETE', bucketName, err.code, 'deleteObject');
            return cb(err, resHeaders);
        }
        if (deleteInfo.newDeleteMarker) {
            // if we created a new delete marker, return true for
            // x-amz-delete-marker and the version ID of the new delete marker
            if (result.versionId) {
                resHeaders['x-amz-delete-marker'] = true;
                resHeaders['x-amz-version-id'] = result.versionId === 'null' ?
                    result.versionId : versionIdUtils.encode(
                        result.versionId, config.versionIdEncodingType);
            }
            pushMetric('putDeleteMarkerObject', log, {
                authInfo,
                bucket: bucketName,
                keys: [objectKey],
                versionId: result.versionId,
                location: objectMD ? objectMD.dataStoreName : undefined,
            });
        } else {
            log.end().addDefaultFields({
                contentLength: objectMD['content-length'],
            });
            pushMetric('deleteObject', log, {
                authInfo,
                canonicalID: bucketMD.getOwner(),
                bucket: bucketName,
                keys: [objectKey],
                byteLength: Number.parseInt(objectMD['content-length'], 10),
                numberOfObjects: 1,
                location: objectMD.dataStoreName,
                isDelete: true,
            });
            monitoring.promMetrics('DELETE', bucketName, '200', 'deleteObject',
                Number.parseInt(objectMD['content-length'], 10));
        }
        return cb(err, resHeaders);
    });
}

module.exports = objectDelete;
