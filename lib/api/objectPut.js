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
 * @param  {datastore}   datastore - data storage endpoint
 * @param  {metastore}   metastore - metadata storage endpoint
 * @param  {request}   request   - request object given by router,
 * includes normalized headers
 * @param  {Function} callback  - final callback to call with the result
 * @return {Function} calls callback from router
 * with err and result as arguments
 */
export default
function objectPut(accessKey, datastore, metastore, request, callback) {
    const bucketname = utils.getResourceNames(request).bucket;
    const objectKey = utils.getResourceNames(request).object;
    const contentMD5 = request.calculatedMD5;
    const metaHeaders = utils.getMetaHeaders(request.lowerCaseHeaders);
    const objectUID =
    utils.getResourceUID(request.namespace, bucketname + objectKey);
    const bucketUID = utils.getResourceUID(request.namespace, bucketname);
    const metadataValParams = {
        accessKey: accessKey,
        bucketUID: bucketUID,
        objectKey: objectKey,
        metastore: metastore
    };
    const dataStoreParams = {
        contentMD5: contentMD5,
        headers: request.lowerCaseHeaders,
        value: request.post,
        objectUID: objectUID
    };
    const metadataStoreParams = {
        objectKey: objectKey,
        accessKey: accessKey,
        objectUID: objectUID,
        metaHeaders: metaHeaders,
        headers: request.lowerCaseHeaders,
        contentMD5: contentMD5};

    async.waterfall([
        function waterfall1(next) {
            services.metadataValidateAuthorization(metadataValParams, next);
        },
        function waterfall2(bucket, objectMetadata, next) {
            services.dataStore(bucket, objectMetadata,
            datastore, dataStoreParams, next);
        },
        function waterfall3(bucket, objectMetadata, newLocation, next) {
            services.metadataStoreObject(bucket, objectMetadata,
            newLocation, metastore, metadataStoreParams, next);
        }
    ], function watefallFinal(err, result) {
        return callback(err, result);
    });
}
