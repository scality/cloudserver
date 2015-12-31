import async from 'async';

import services from '../services';
import utils from '../utils';

/**
 * HEAD Object - Same as Get Object but only respond with headers
 *(no actual body)
 * @param  {string} accessKey - user's access key
 * @param {object} metastore - metastore with buckets
 * containing objects and their metadata
 * @param {object} request - normalized request object
 * @param  {function} log - Werelogs logger
 * @param {function} callback - callback to function in route
 * @return {function} callback with error and responseMetaHeaders as arguments
 *
 */
export default function objectHead(accessKey, metastore, request, log,
    callback) {
    const bucketName = utils.getResourceNames(request).bucket;
    const objectKey = utils.getResourceNames(request).object;
    const metadataValParams = {
        accessKey,
        bucketName,
        objectKey,
        metastore,
        requestType: 'objectHead',
        log,
    };
    const metadataCheckParams = {headers: request.lowerCaseHeaders};

    async.waterfall([
        function waterfall1(next) {
            services.metadataValidateAuthorization(metadataValParams, next);
        },
        function waterfall2(bucket, objectMetadata, next) {
            services.metadataChecks(objectMetadata, metadataCheckParams, next);
        }
    ], function finalfunc(err, objectMetadata, responseMetaHeaders) {
        return callback(err, responseMetaHeaders);
    });
}
