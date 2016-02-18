import services from '../services';
import utils from '../utils';

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
 * @param {Function} callback - final callback to call with the result
 * @return {undefined}
 */
export default
function objectPut(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'objectPut' });

    const bucketName = request.bucketName;
    const objectKey = request.objectKey;
    const metaHeaders = utils.getMetaHeaders(request.lowerCaseHeaders);
    log.trace('meta headers', { metaHeaders });
    const size = request.lowerCaseHeaders['content-length'];
    const contentType = request.lowerCaseHeaders['content-type'];
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
    services.metadataValidateAuthorization(valParams, (err, bucket, objMD) => {
        if (err) {
            log.warn('error processing request', {
                error: err,
                method: 'services.metadataValidateAuthorization',
            });
            return callback(err);
        }
        log.trace('storing object in data');
        services.dataStore(objMD, objectKeyContext, request, log,
            (err, objMD, keys) => {
                if (err) {
                    log.warn('error from data', {
                        error: err,
                        method: 'services.dataStore',
                    });
                    return callback(err);
                }
                const contentMD5 = request.calculatedHash;
                const metadataStoreParams = {
                    objectKey,
                    authInfo,
                    metaHeaders,
                    size,
                    contentType,
                    contentMD5,
                    headers: request.lowerCaseHeaders,
                    log,
                };
                services.metadataStoreObject(bucketName, objMD, keys,
                                         metadataStoreParams, callback);
            });
    });
}
