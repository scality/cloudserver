import { errors } from 'arsenal';

import services from '../services';
import constants from '../../constants';

/*
 * Private function
 */
function doStore(objectKeyContext, request, log,
                 partNumber, size, uploadId, cb) {
    return function mdValidateMPU(err, mpuBucket) {
        if (err) {
            return cb(err);
        }
        return services.dataStore(
            null, objectKeyContext, request, size, log, (err, extraArg,
                dataGetInfo) => {
                if (err) {
                    return cb(err);
                }
                let splitter = constants.splitter;
                // BACKWARD: Remove to remove the old splitter
                if (mpuBucket.getMdBucketModelVersion() < 2) {
                    splitter = constants.oldSplitter;
                }
                const mdParams = {
                    partNumber,
                    contentMD5: request.calculatedHash,
                    size,
                    uploadId,
                    splitter,
                };
                return services.metadataStorePart(mpuBucket.getName(),
                                           dataGetInfo, mdParams, log, cb);
            });
    };
}

/**
 * PUT part of object during a multipart upload. Steps include:
 * validating metadata for authorization, bucket existence
 * and multipart upload initiation existence,
 * store object data in datastore upon successful authorization,
 * store object location returned by datastore in metadata and
 * return the result in final cb
 *
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - request object
 * @param {object} log - Werelogs logger
 * @param {function} cb - final callback to call with the result
 * @return {undefined}
 */
export default function objectPutPart(authInfo, request, log, cb) {
    log.debug('processing request', { method: 'objectPutPart' });

    const bucketName = request.bucketName;
    const objectKey = request.objectKey;
    const size = request.parsedContentLength;
    const partNumber = Number.parseInt(request.query.partNumber, 10);
    // AWS caps partNumbers at 10,000
    if (partNumber > 10000) {
        return cb(errors.TooManyParts);
    }
    if (!Number.isInteger(partNumber)) {
        return cb(errors.InvalidArgument);
    }
    // If part size is greater than 5GB, reject it
    if (Number.parseInt(size, 10) > 5368709120) {
        return cb(errors.EntityTooLarge);
    }
    // Note: Parts are supposed to be at least 5MB except for last part.
    // However, there is no way to know whether a part is the last part
    // since keep taking parts until get a completion request.  But can
    // expect parts of at least 5MB until last part.  Also, we check that
    // part sizes are large enough when mutlipart upload completed.

    // Note that keys in the query object retain their case, so
    // request.query.uploadId must be called with that exact
    // capitalization
    const uploadId = request.query.uploadId;
    const metadataValMPUparams = {
        authInfo,
        bucketName,
        objectKey,
        uploadId,
        requestType: 'putPart or complete',
        log,
        splitter: constants.splitter,
    };
    // For validating the request at the destinationBucket level
    // params are the same as validating at the MPU level
    // but the requestType is the more general 'objectPut'
    const metadataValParams = Object.assign({}, metadataValMPUparams);
    metadataValParams.requestType = 'objectPut';
    log.trace('owner canonicalid to send to data', {
        canonicalID: authInfo.getCanonicalID,
    });
    const objectKeyContext = {
        bucketName,
        owner: authInfo.getCanonicalID(),
        namespace: request.namespace,
    };

    return services.metadataValidateAuthorization(metadataValParams,
        (err, bucket) => {
            if (err) {
                return cb(err);
            }
            return services.getMPUBucket(bucket, bucketName, log,
                (err, mpuBucket) => {
                    if (err) {
                        return cb(err);
                    }
                    // BACKWARD: Remove to remove the old splitter
                    if (mpuBucket.getMdBucketModelVersion() < 2) {
                        metadataValMPUparams.splitter = constants.oldSplitter;
                    }
                    // We pad the partNumbers so that the parts will be sorted
                    // in numerical order
                    const paddedPartNumber = `000000${partNumber}`.substr(-5);
                    return services.metadataValidateMultipart(
                        metadataValMPUparams,
                            doStore(objectKeyContext, request, log,
                                paddedPartNumber, size, uploadId, cb));
                });
        });
}
