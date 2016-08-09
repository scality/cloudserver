import { errors } from 'arsenal';

import data from '../data/wrapper';
import services from '../services';
import utils from '../utils';
import { cleanUpBucket } from './apiUtils/bucket/bucketCreation';
import constants from '../../constants';
import { logger } from '../utilities/logger';

function _storeInMDandDeleteData(bucketName, dataGetInfo,
    metadataStoreParams, dataToDelete, deleteLog, callback) {
    services.metadataStoreObject(bucketName, dataGetInfo,
        metadataStoreParams, (err, contentMD5) => {
            if (err) {
                return callback(err);
            }
            if (dataToDelete) {
                data.batchDelete(dataToDelete, deleteLog);
            }
            return callback(null, contentMD5);
        });
}

function _storeIt(bucketName, objectKey, objMD, authInfo, canonicalID,
    request, log, callback) {
    const size = request.parsedContentLength;
    const contentType = request.headers['content-type'];
    const metaHeaders = utils.getMetaHeaders(request.headers);
    log.trace('meta headers', { metaHeaders, method: 'objectPut' });
    const objectKeyContext = {
        bucketName,
        owner: canonicalID,
        namespace: request.namespace,
    };
    const metadataStoreParams = {
        objectKey,
        authInfo,
        metaHeaders,
        size,
        contentType,
        headers: request.headers,
        log,
    };
    let dataToDelete;
    if (objMD && objMD.location) {
        dataToDelete = Array.isArray(objMD.location) ?
            objMD.location : [objMD.location];
    }
    if (size !== 0) {
        log.trace('storing object in data', {
            method: 'services.metadataValidateAuthorization',
        });
        return services.dataStore(objMD, objectKeyContext, request, log,
            (err, objMD, dataGetInfo) => {
                if (err) {
                    log.trace('error from data', {
                        error: err,
                        method: 'services.dataStore',
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
                metadataStoreParams.contentMD5 = request.calculatedHash;
                return _storeInMDandDeleteData(bucketName,
                    dataGetInfoArr, metadataStoreParams, dataToDelete,
                        logger.newRequestLoggerFromSerializedUids(log
                        .getSerializedUids()), callback);
            });
    }
    log.trace('content-length is 0 so only storing metadata', {
        method: 'services.metadataValidateAuthorization',
    });
    metadataStoreParams.contentMD5 = constants.emptyFileMd5;
    const dataGetInfo = null;
    return _storeInMDandDeleteData(bucketName, dataGetInfo,
        metadataStoreParams, dataToDelete,
            logger.newRequestLoggerFromSerializedUids(log
            .getSerializedUids()), callback);
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
 * @param {object} log - the log request
 * @param {Function} callback - final callback to call with the result
 * @return {undefined}
 */
export default
function objectPut(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'objectPut' });
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
                        return _storeIt(bucketName, objectKey, objMD,
                            authInfo, canonicalID, request, log, callback);
                    });
        }
        return _storeIt(bucketName, objectKey, objMD, authInfo, canonicalID,
            request, log, callback);
    });
}
