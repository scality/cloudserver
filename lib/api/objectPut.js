import data from '../data/wrapper';
import services from '../services';
import utils from '../utils';
import constants from '../../constants';
import { logger } from '../utilities/logger';

function _storeInMDandDeleteData(bucketName, objMD, dataGetInfo,
    metadataStoreParams, dataToDelete, deleteLog, callback) {
    services.metadataStoreObject(bucketName, objMD, dataGetInfo,
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
    const metaHeaders = utils.getMetaHeaders(request.headers);
    log.trace('meta headers', { metaHeaders });
    const size = request.headers['content-length'];
    const contentType = request.headers['content-type'];
    const valParams = {
        authInfo,
        bucketName,
        objectKey,
        requestType: 'objectPut',
        log,
    };
    log.trace('owner canonicalID to send to data',
        { canonicalID: authInfo.getCanonicalID() });
    const objectKeyContext = {
        bucketName,
        owner: authInfo.getCanonicalID(),
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
    services.metadataValidateAuthorization(valParams, (err, bucket, objMD) => {
        if (err) {
            log.warn('error processing request', {
                error: err,
                method: 'services.metadataValidateAuthorization',
            });
            return callback(err);
        }
        let dataToDelete;
        if (objMD && objMD.location) {
            dataToDelete = Array.isArray(objMD.location) ?
                objMD.location : [objMD.location];
        }
        if (size !== '0') {
            log.trace('storing object in data');
            services.dataStore(objMD, objectKeyContext, request, log,
                (err, objMD, dataGetInfo) => {
                    if (err) {
                        log.warn('error from data', {
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
                    return _storeInMDandDeleteData(bucketName, objMD,
                        dataGetInfoArr, metadataStoreParams, dataToDelete,
                            logger.newRequestLoggerFromSerializedUids(log
                            .getSerializedUids()), callback);
                });
        } else {
            log.trace('content-length is 0 so only storing metadata');
            metadataStoreParams.contentMD5 = constants.emptyFileMd5;
            const dataGetInfo = null;
            return _storeInMDandDeleteData(bucketName, objMD, dataGetInfo,
                metadataStoreParams, dataToDelete,
                    logger.newRequestLoggerFromSerializedUids(log
                    .getSerializedUids()), callback);
        }
    });
}
