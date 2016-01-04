import async from 'async';

import Config from '../../lib/Config';
import data from '../data/wrapper';
import services from '../services';
import utils from '../utils';

const splitter = new Config().splitter;

/**
 * multipartDelete - DELETE an open multipart upload from a bucket
 * @param  {string}   accessKey - user access key
 * @param  {object}   metastore - metadata storage endpoint
 * @param  {object}   request   - request object given by router,
 * includes normalized headers
 * @param  {function} cb  - final callback to call with the
 * result and response headers
 * @return {function} calls callback from router
 * with err, result and responseMetaHeaders as arguments
 */
export default
function multipartDelete(accessKey, metastore, request, callback) {
    const resourceRes = utils.getResourceNames(request);
    const bucketName = resourceRes.bucket;
    const objectKey = resourceRes.object;
    const uploadId = request.query.uploadId;
    const metadataValParams = {
        accessKey,
        bucketName,
        objectKey,
        metastore,
        uploadId,
        requestType: 'deleteMPU',
    };
    async.waterfall([
        function waterfall1(next) {
            services.checkBucketPolicies(metadataValParams, next);
        },
        function waterfall2(bucketPolicyGoAhead, next) {
            if (bucketPolicyGoAhead === 'accessGranted') {
                metadataValParams.requestType = 'bucketPolicyGoAhead';
            }
            services.metadataValidateMultipart(metadataValParams, next);
        },
        function waterfall3(mpuBucket, mpuOverviewArray, next) {
            services.getMPUparts(mpuBucket.name, uploadId,
                (err, storedParts) => {
                    if (err) {
                        return next(err);
                    }
                    return next(null, mpuBucket, storedParts, mpuOverviewArray);
                });
        },
        function waterfall4(mpuBucket, storedParts, mpuOverviewArray, next) {
            const locations = storedParts.map((item) => {
                return item.key.split(splitter)[5];
            });
            data.delete(locations, (err) => {
                if (err) {
                    return next(err);
                }
                return next(null, mpuBucket, storedParts, mpuOverviewArray);
            });
        },
        function waterfall5(mpuBucket, storedParts, mpuOverviewArray, next) {
            const mpuOverviewKey = mpuOverviewArray.join(splitter);
            const keysToDelete = storedParts.map((item) => {
                return item.key;
            });
            keysToDelete.push(mpuOverviewKey);
            services.batchDeleteObjectMetadata(mpuBucket.name,
                keysToDelete, (err) => {
                    return next(err);
                });
        },
    ], function waterfallFinal(err) {
        return callback(err);
    });
}
