import utils from '../utils.js';
import services from '../services.js';
import async from 'async';


/**
 * GET Object - Get an object
 * @param  {string} accessKey - user's access key
 * @param {object} datastore - datastore with object data
 * @param {object} metastore - metastore with buckets containing
 * objects and their metadata
 * @param {object} request - normalized request object
 * @param {function} callback - callback to function in route
 * @return {function} callback with error, object data result
 * and responseMetaHeaders as arguments
 *
 */

export default
function objectGet(accessKey, datastore,  metastore, request, callback) {
    const bucketname = utils.getResourceNames(request).bucket;
    const bucketUID = utils.getResourceUID(request.namespace, bucketname);
    const objectKey = utils.getResourceNames(request).object;
    const metadataValParams = {
        accessKey,
        bucketUID,
        objectKey,
        metastore,
        requestType: 'objectGet',
    };
    const metadataCheckParams = {headers: request.lowerCaseHeaders};

    async.waterfall([
        function waterfall1(next) {
            services.metadataValidateAuthorization(metadataValParams, next);
        },
        function waterfall2(bucket, objectMetadata, next) {
            services.metadataChecks(bucket, objectMetadata,
            metadataCheckParams, next);
        },
        function waterfall3(bucket, objectMetadata, metaHeaders, next) {
            services.getFromDatastore(bucket, objectMetadata,
            metaHeaders, datastore, next);
        }
    ], function waterfallFinal(err, result, responseMetaHeaders) {
        return callback(err, result, responseMetaHeaders);
    });
}
