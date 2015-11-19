import utils from '../utils.js';
import services from '../services.js';
import async from 'async';

/**
 * multipartDelete - DELETE an open multipart upload from a bucket
 * @param  {string}   accessKey - user access key
 * @param  {object}   datastore - data storage endpoint
 * @param  {object}   metastore - metadata storage endpoint
 * @param  {object}   request   - request object given by router,
 * includes normalized headers
 * @param  {function} callback  - final callback to call with the
 * result and response headers
 * @return {function} calls callback from router
 * with err, result and responseMetaHeaders as arguments
 */

export default
function multipartDelete(accessKey, datastore,  metastore, request, callback) {
    const resourceRes = utils.getResourceNames(request);
    const bucketname = resourceRes.bucket;
    const bucketUID = utils.getResourceUID(request.namespace, bucketname);
    const objectKey = resourceRes.object;
    const uploadId = request.query.uploadId;
    const metadataValParams = {
        accessKey,
        bucketUID,
        objectKey,
        metastore,
        uploadId,
        requestType: 'delete',
    };
    async.waterfall([
        function waterfall1(next) {
            services.metadataValidateMultipart(metadataValParams, next);
        },
        function waterfall2(bucket, multipartMetadata, next) {
            const locations = multipartMetadata.partLocations.map((item) => {
                return item.location;
            });
            services.deleteData(locations, datastore, (err) =>{
                if (err) {
                    return next(err);
                }
                return next(null, bucket);
            });
        },
        function waterfall3(bucket, next) {
            services.deleteMultipartUploadEntry(bucket, uploadId, next);
        },
    ], function waterfallFinal(err) {
        return callback(err);
    });
}
