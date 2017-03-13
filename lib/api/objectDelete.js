import { errors, versioning } from 'arsenal';
import async from 'async';

import collectCorsHeaders from '../utilities/collectCorsHeaders';
import services from '../services';
import validateHeaders from '../utilities/validateHeaders';
import { pushMetric } from '../utapi/utilities';
import { cleanUpBucket } from './apiUtils/bucket/bucketCreation';
import { createAndStoreObject } from './objectPut';

const VID = versioning.VersionID;

/**
 * objectDelete - DELETE an object from a bucket
 * (currently supports only non-versioned buckets)
 * @param {AuthInfo} authInfo - requester's infos
 * @param {object} request - request object given by router,
 *                           includes normalized headers
 * @param {Logger} log - werelogs request instance
 * @param {function} cb - final cb to call with the result and response headers
 * @return {undefined}
 */
export default function objectDelete(authInfo, request, log, cb) {
    log.debug('processing request', { method: 'objectDelete' });
    if (authInfo.isRequesterPublicUser()) {
        log.debug('operation not available for public user');
        return cb(errors.AccessDenied);
    }
    const bucketName = request.bucketName;
    const objectKey = request.objectKey;
    let reqVersionId = request.query ? request.query.versionId : undefined;

    if (reqVersionId && reqVersionId !== 'null') {
        try {
            reqVersionId = VID.decrypt(reqVersionId);
        } catch (exception) { // eslint-disable-line
            return cb(errors.InvalidArgument.customizeDescription(
                    'Invalid version id specified'), null);
        }
    }

    const valParams = {
        authInfo,
        bucketName,
        objectKey,
        versionId: reqVersionId || undefined,
        requestType: 'objectDelete',
        log,
    };

    const canonicalID = authInfo.getCanonicalID();
    let bucketMD = undefined;
    let objectMD = undefined;
    let corsHeaders = undefined;
    let removeDeleteMarker = false;
    let deleteOptions = undefined;
    return async.waterfall([
        callback => services.metadataValidateAuthorization(valParams, callback),
        (bucket, objMD, callback) => {
            corsHeaders = collectCorsHeaders(request.headers.origin,
                    request.method, bucket);
            bucketMD = bucket;
            objectMD = objMD;
            const versioning = bucketMD.getVersioningConfiguration();
            // TODO adhere to the behaviour of AWS: create a delete marker
            // even if the deleting object does not exist
            if (!objMD) {
                if (versioning) {
                    if (reqVersionId) {
                        corsHeaders['x-amz-version-id'] =
                            VID.encrypt(reqVersionId);
                        return cb(null, corsHeaders, null);
                    }
                } else {
                    return callback(errors.NoSuchKey);
                }
            }
            if (objMD) {
                const headerValResult = validateHeaders(objMD, request.headers);
                if (headerValResult.error) {
                    return callback(headerValResult.error);
                }
                if (objMD['content-length']) {
                    log.end().addDefaultFields({
                        contentLength: objMD['content-length'],
                    });
                }
            }
            return callback();
        },
        callback => services.preprocessingVersioningDelete(bucketName,
            bucketMD, objectKey, objectMD, reqVersionId, log, callback),
        (options, callback) => {
            if (options && options.deleteData) {
                // delete object
                deleteOptions = options;
                return callback(null, options);
            }
            // putting a new delete marker
            if (bucketMD.hasDeletedFlag() &&
                    canonicalID !== bucketMD.getOwner()) {
                log.trace('deleted flag on bucket and request ' +
                        'from non-owner account');
                return callback(errors.NoSuchBucket);
            }
            if (bucketMD.hasTransientFlag() || bucketMD.hasDeletedFlag()) {
                return cleanUpBucket(bucketMD, canonicalID,
                        log, err => callback(err, null));
            }
            return callback(null, null);
        },
        (options, callback) => {
            if (options && options.deleteData) {
                if (objectMD.isDeleteMarker) {
                    removeDeleteMarker = true;
                }
                return services.deleteObject(bucketName, objectMD, objectKey,
                        options, log, callback);
            }
            request.isDeleteMarker = true; // eslint-disable-line
            return createAndStoreObject(bucketName, bucketMD,
                objectKey, objectMD, authInfo, canonicalID, null, request,
                null, log, callback);
        },
    ], (err, res) => {
        if (err) {
            log.debug('error processing request', { error: err,
                method: 'metadataValidateAuthorization' });
        } else if (deleteOptions === undefined) {
            // TODO metric for delete marker
            if (res.versionId) {
                corsHeaders['x-amz-delete-marker'] = true;
                corsHeaders['x-amz-version-id'] = res.versionId === 'null' ?
                    res.versionId : VID.encrypt(res.versionId);
            }
            pushMetric('putObject', log, { authInfo, bucket: bucketName,
                newByteLength: 0, oldByteLength: 0 });
        } else {
            if (reqVersionId) {
                corsHeaders['x-amz-version-id'] = reqVersionId === 'null' ?
                    reqVersionId : VID.encrypt(reqVersionId);
                if (removeDeleteMarker) {
                    corsHeaders['x-amz-delete-marker'] = true;
                }
            }
            pushMetric('deleteObject', log, { authInfo, bucket: bucketName,
                byteLength: objectMD['content-length'], numberOfObjects: 1 });
        }
        return cb(err, corsHeaders);
    });
}
