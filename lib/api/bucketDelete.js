import utils from '../utils.js';
import services from '../services.js';
import async from 'async';

/**
 * bucketDelete - DELETE bucket (currently supports only non-versioned buckets)
 * @param  {string}   accessKey - user access key
 * @param  {object}   metastore - metadata storage endpoint
 * @param  {object}   request   - request object given by router
 *  including normalized headers
 * @param  {function} callback  - final callback to call
 *  with the result and response headers
 * @return {function} calls callback from router
 *  with err, result and responseMetaHeaders as args
 */
export default function bucketDelete(accessKey,  metastore, request, callback) {
    const resourceRes = utils.getResourceNames(request);
    const bucketname = resourceRes.bucket;
    const bucketUID = utils.getResourceUID(request.namespace, bucketname);
    const metadataValParams = {
        accessKey: accessKey,
        bucketUID: bucketUID,
        metastore: metastore
    };

    async.waterfall([
        function waterfall1(next) {
            services.metadataValidateAuthorization(metadataValParams, next);
        },
        function waterfall2(bucket, responseMetaHeaders, next) {
            services.deleteBucket(bucket, responseMetaHeaders,
            metastore, bucketUID, accessKey, next);
        }
    ], function waterfallFinal(err, result, responseMetaHeaders) {
        return callback(err, result, responseMetaHeaders);
    });
}
