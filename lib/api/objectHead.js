import utils from '../utils.js';
import services from '../services.js';
import async from 'async';


/**
 * HEAD Object - Same as Get Object but only respond with headers
 *(no actual body)
 * @param  {string} accessKey - user's access key
 * @param {object} metastore - metastore with buckets
 * containing objects and their metadata
 * @param {object} request - normalized request object
 * @param {function} callback - callback to function in route
 * @return {function} callback with error and responseMetaHeaders as arguments
 *
 */

export default function objectHead(accessKey,  metastore, request, callback) {
    const bucketname = utils.getResourceNames(request).bucket;
    const bucketUID = utils.getResourceUID(request.namespace, bucketname);
    const objectKey = utils.getResourceNames(request).object;
    const metadataValParams = {
        accessKey,
        bucketUID,
        objectKey,
        metastore,
        requestType: 'objectHead',
    };
    const metadataCheckParams = {headers: request.lowerCaseHeaders};

    async.waterfall([
        function waterfall1(next) {
            services.metadataValidateAuthorization(metadataValParams, next);
        },
        function waterfall2(bucket, objectMetadata, next) {
            services.metadataChecks(bucket, objectMetadata,
            metadataCheckParams, next);
        }
    ], function finalfunc(err, bucket, objectMetadata, responseMetaHeaders) {
        return callback(err, responseMetaHeaders);
    });
}
