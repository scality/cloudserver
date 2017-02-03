import async from 'async';
import { errors } from 'arsenal';

import constants from '../../constants';
import data from '../data/wrapper';
import services from '../services';

/**
 * multipartDelete - DELETE an open multipart upload from a bucket
 * @param  {AuthInfo} authInfo -Instance of AuthInfo class with requester's info
 * @param  {object} request - request object given by router,
 *                            includes normalized headers
 * @param  {object} log - the log request
 * @param  {function} callback - final callback to call with the
 *                          result and response headers
 * @return {undefined} calls callback from router
 * with err, result and responseMetaHeaders as arguments
 */
export default
function multipartDelete(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'multipartDelete' });

    const bucketName = request.bucketName;
    const objectKey = request.objectKey;
    const uploadId = request.query.uploadId;
    const metadataValMPUparams = {
        authInfo,
        bucketName,
        objectKey,
        uploadId,
        requestType: 'deleteMPU',
        log,
    };
    // For validating the request at the destinationBucket level
    // params are the same as validating at the MPU level
    // but the requestType is the more general 'objectDelete'
    const metadataValParams = Object.assign({}, metadataValMPUparams);
    metadataValParams.requestType = 'objectPut';

    async.waterfall([
        function checkDestBucketVal(next) {
            services.metadataValidateAuthorization(metadataValParams,
                (err, destinationBucket) => {
                    if (err) {
                        return next(err);
                    }
                    if (destinationBucket.policies) {
                        // TODO: Check bucket policies to see if user is granted
                        // permission or forbidden permission to take
                        // given action.
                        // If permitted, add 'bucketPolicyGoAhead'
                        // attribute to params for validating at MPU level.
                        // This is GH Issue#76
                        metadataValMPUparams.requestType =
                            'bucketPolicyGoAhead';
                    }
                    return next();
                });
        },
        function checkMPUval(next) {
            services.metadataValidateMultipart(metadataValParams,
                (err, mpuBucket, mpuOverviewArray) => {
                    if (err === errors.NoSuchUpload) {
                        log.trace('did not find valid mpu with uploadId' +
                        `${uploadId}`, { method: 'multipartDelete' });
                        // TODO in rel/6.3 and later: return 404 if
                        // usEastBehavior is enabled in config
                        return callback();
                    }
                    return next(err, mpuBucket, mpuOverviewArray);
                });
        },
        function getPartLocations(mpuBucket, mpuOverviewArray, next) {
            services.getMPUparts(mpuBucket.getName(), uploadId, log,
                (err, result) => {
                    if (err) {
                        return next(err);
                    }
                    const storedParts = result.Contents;
                    return next(null, mpuBucket, storedParts);
                });
        },
        function deleteData(mpuBucket, storedParts, next) {
            // The locations were sent to metadata as an array
            // under partLocations.  Pull the partLocations.
            let locations = storedParts.map(item => item.value.partLocations);
            if (locations.length === 0) {
                return next(null, mpuBucket, storedParts);
            }
            // flatten the array
            locations = [].concat.apply([], locations);
            return async.eachLimit(locations, 5, (loc, cb) => {
                data.delete(loc, log, err => {
                    if (err) {
                        log.fatal('delete ObjectPart failed', { err });
                    }
                    cb();
                });
            }, () => next(null, mpuBucket, storedParts));
        },
        function deleteMetadata(mpuBucket, storedParts, next) {
            let splitter = constants.splitter;
            // BACKWARD: Remove to remove the old splitter
            if (mpuBucket.getMdBucketModelVersion() < 2) {
                splitter = constants.oldSplitter;
            }
            // Reconstruct mpuOverviewKey
            const mpuOverviewKey =
                `overview${splitter}${objectKey}${splitter}${uploadId}`;
            const keysToDelete = storedParts.map(item => item.key);
            keysToDelete.push(mpuOverviewKey);
            services.batchDeleteObjectMetadata(mpuBucket.getName(),
                keysToDelete, log, err => next(err));
        },
    ], callback);
}
