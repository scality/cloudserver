import utils from '../utils.js';
import services from '../services.js';
import async from 'async';

/**
 * PUT Object in the requested bucket. Steps include:
 * validating metadata for authorization, bucket and object existence etc.
 * store object data in datastore upon successful authorization
 * store object location returned by datastore and
 *object's (custom) headers in metadata
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
function objectPut(accessKey, metastore, request, callback) {
    const bucketname = utils.getResourceNames(request).bucket;
    const objectKey = utils.getResourceNames(request).object;
    const contentMD5 = request.calculatedMD5;
    const metaHeaders = utils.getMetaHeaders(request.lowerCaseHeaders);
    const objectUID =
    utils.getResourceUID(request.namespace, bucketname + objectKey);
    const bucketUID = utils.getResourceUID(request.namespace, bucketname);
    const size = request.lowerCaseHeaders['content-length'];
    const contentType = request.lowerCaseHeaders['content-type'];
    const metadataValParams = {
        accessKey,
        bucketUID,
        objectKey,
        metastore,
        requestType: 'objectPut',
    };
    const metadataStoreParams = {
        objectKey,
        accessKey,
        objectUID,
        metaHeaders,
        size,
        contentType,
        contentMD5,
        headers: request.lowerCaseHeaders,
    };
    async.waterfall([
        function waterfall1(next) {
            services.metadataValidateAuthorization(metadataValParams, next);
        },
        function waterfall2(bucket, objectMetadata, next) {
            services.dataStore(bucket, objectMetadata,
                               Buffer.concat(request.post), next);
        },
        function waterfall3(bucket, objectMetadata, keys, next) {
            services.metadataStoreObject(bucket, objectMetadata, keys,
                                         metadataStoreParams, next);
        }
    ], function watefallFinal(err, result) {
        return callback(err, result);
    });
}
