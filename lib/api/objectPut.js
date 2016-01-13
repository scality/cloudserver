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
 * @param  {string}   accessKey - user access key
 * @param  {metastore}   metastore - metadata storage endpoint
 * @param  {request}   request   - request object given by router,
 * includes normalized headers
 * @param  {Function} callback  - final callback to call with the result
 * @return {Function} calls callback from router
 * with err and result as arguments
 */
export default
function objectPut(accessKey, metastore, request, log, callback) {
    const bucketName = utils.getResourceNames(request).bucket;
    const objectKey = utils.getResourceNames(request).object;
    const contentMD5 = request.calculatedMD5;
    const metaHeaders = utils.getMetaHeaders(request.lowerCaseHeaders);
    const size = request.lowerCaseHeaders['content-length'];
    const contentType = request.lowerCaseHeaders['content-type'];
    const valParams = {
        accessKey,
        bucketName,
        objectKey,
        metastore,
        requestType: 'objectPut',
        log,
    };
    const metadataStoreParams = {
        objectKey,
        accessKey,
        metaHeaders,
        size,
        contentType,
        contentMD5,
        headers: request.lowerCaseHeaders,
        log,
    };
    const objectKeyContext = {
        bucketName,
        owner: accessKey,
        namespace: request.namespace,
    };
    services.metadataValidateAuthorization(valParams, (err, bucket, objMD) => {
        if (err) {
            return callback(err);
        }
        services.dataStore(
            objMD, objectKeyContext, request.post,
            (err, objMD, keys) => {
                if (err) {
                    return callback(err);
                }
                services.metadataStoreObject(bucketName, objMD, keys,
                                         metadataStoreParams, callback);
            });
    });
}
