import async from 'async';

import constants from '../../constants';
import data from '../data/wrapper';
import services from '../services';

const splitter = constants.splitter;

/**
 * multipartDelete - DELETE an open multipart upload from a bucket
 * @param  {AuthInfo} Instance of AuthInfo class with requester's info
 * @param  {object}   request   - request object given by router,
 * includes normalized headers
 * @param  {function} cb  - final callback to call with the
 * result and response headers
 * @return {function} calls callback from router
 * with err, result and responseMetaHeaders as arguments
 */
export default
function multipartDelete(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'multipartDelete' });

    const bucketName = request.bucketName;
    const objectKey = request.objectKey;
    const uploadId = request.query.uploadId;
    const metadataValParams = {
        authInfo,
        bucketName,
        objectKey,
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
                    return next(null, mpuBucket, storedParts);
                });
        },
        function waterfall4(mpuBucket, storedParts, next) {
            // The locations were sent to metadata as an array
            // under partLocations.  Pull the partLocations.
            const locations = storedParts.map(item => item.value.partLocations);
            if (locations.length === 0) {
                return next(null, mpuBucket, storedParts);
            }
            // If have locations, flatten the array
            async.each(locations, (loc, cb) => {
                data.delete(loc, log, (err) => {
                    if (err) {
                        log.fatal('delete ObjectPart failed', { err });
                    }
                    cb();
                });
            }, () => {
                return next(null, mpuBucket, storedParts);
            });
        },
        function waterfall5(mpuBucket, storedParts, next) {
            // Reconstruct mpuOverviewKey
            const mpuOverviewKey =
                `overview${splitter}${objectKey}${splitter}${uploadId}`;
            const keysToDelete = storedParts.map(item => item.key);
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
