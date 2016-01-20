import async from 'async';

import services from '../services';
import utils from '../utils';

/**
 * GET Object - Get an object
 * @param  {string} accessKey - user's access key
 * @param {object} metastore - metastore with buckets containing
 * objects and their metadata
 * @param {object} request - normalized request object
 * @param {function} callback - callback to function in route
 * @return {function} callback with error, object data result
 * and responseMetaHeaders as arguments
 */
export default
function objectGet(accessKey, metastore, request, log, callback) {
    const bucketName = utils.getResourceNames(request).bucket;
    const objectKey = utils.getResourceNames(request).object;
    const metadataValParams = {
        accessKey,
        bucketName,
        objectKey,
        metastore,
        requestType: 'objectGet',
        log,
    };
    const validateHeadersParams = { headers: request.lowerCaseHeaders };

    async.waterfall([
        function waterfall1(next) {
            services.metadataValidateAuthorization(metadataValParams,
                (err, bucket, objectMetadata) => {
                    if (!objectMetadata) {
                        return next('NoSuchKey');
                    }
                    return next(null, bucket, objectMetadata);
                });
        },
        function waterfall2(bucket, objectMetadata, next) {
            services.validateHeaders(objectMetadata, validateHeadersParams,
                next);
        },
        function waterfall3(objectMetadata, metaHeaders, next) {
            services.getFromDatastore(objectMetadata, metaHeaders, log, next);
        }
    ], function waterfallFinal(err, result, responseMetaHeaders) {
        return callback(err, result, responseMetaHeaders);
    });
}
