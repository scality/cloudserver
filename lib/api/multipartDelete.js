import async from 'async';

import constants from '../../constants';
import data from '../data/wrapper';
import services from '../services';

const splitter = constants.splitter;

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
function multipartDelete(accessKey, metastore, request, log, callback) {
    const bucketName = request.bucketName;
    const objectKey = request.objectKey;
    const uploadId = request.query.uploadId;
    const metadataValParams = {
        accessKey,
        bucketName,
        objectKey,
        metastore,
        uploadId,
        requestType: 'deleteMPU',
        log,
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
            services.getMPUparts(mpuBucket.name, uploadId, log,
                (err, storedParts) => {
                    if (err) {
                        return next(err);
                    }
                    return next(null, mpuBucket, storedParts, mpuOverviewArray);
                });
        },
        function waterfall4(mpuBucket, storedParts, mpuOverviewArray, next) {
            const locations = storedParts.map((item) => {
                // The locations were sent to metadata with ',' as
                // a separator
                return item.key.split(splitter)[5].split(',');
            })
                // Mapping results in arrays within an array so need
                // to flatten
                .reduce( (a, b) => {
                    return a.concat(b);
                });
            data.delete(locations, log, (err) => {
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
            services.batchDeleteObjectMetadata(mpuBucket.name, keysToDelete,
                log, (err) => {
                    return next(err);
                });
        },
    ], function waterfallFinal(err) {
        return callback(err);
    });
}
