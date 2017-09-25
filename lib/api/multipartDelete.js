const async = require('async');
const { errors } = require('arsenal');

const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const constants = require('../../constants');
const data = require('../data/wrapper');
const services = require('../services');
const { metadataValidateBucketAndObj } = require('../metadata/metadataUtils');
const { pushMetric } = require('../utapi/utilities');
const isLegacyAWSBehavior = require('../utilities/legacyAWSBehavior');
const { config } = require('../Config');
const multipleBackendGateway = require('../data/multipleBackendGateway');

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
    };
    // For validating the request at the destinationBucket level
    // params are the same as validating at the MPU level
    // but the requestType is the more general 'objectDelete'
    const metadataValParams = Object.assign({}, metadataValMPUparams);
    metadataValParams.requestType = 'objectPut';

    async.waterfall([
        function checkDestBucketVal(next) {
            metadataValidateBucketAndObj(metadataValParams, log,
                (err, destinationBucket) => {
                    if (err) {
                        return next(err, destinationBucket);
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
                    return next(null, destinationBucket);
                });
        },
        function checkMPUval(destBucket, next) {
            metadataValParams.log = log;
            services.metadataValidateMultipart(metadataValParams,
                (err, mpuBucket, mpuOverviewObj) => {
                    if (err) {
                        return next(err, destBucket);
                    }
                    return next(err, mpuBucket, mpuOverviewObj, destBucket);
                });
        },
        function ifMultipleBackend(mpuBucket, mpuOverviewObj, destBucket,
        next) {
            if (config.backends.data === 'multiple') {
                const location = mpuOverviewObj.
                    controllingLocationConstraint;
                return multipleBackendGateway.abortMPU(objectKey, uploadId,
                location, bucketName, log, (err, skipDataDelete) => {
                    if (err) {
                        return next(err, destBucket);
                    }
                    return next(null, mpuBucket, destBucket,
                    skipDataDelete);
                });
            }
            return next(null, mpuBucket, destBucket, false);
        },
        function getPartLocations(mpuBucket, destBucket, skipDataDelete,
        next) {
            services.getMPUparts(mpuBucket.getName(), uploadId, log,
                (err, result) => {
                    if (err) {
                        return next(err, destBucket);
                    }
                    const storedParts = result.Contents;
                    return next(null, mpuBucket, storedParts, destBucket,
                    skipDataDelete);
                });
        },
        function deleteData(mpuBucket, storedParts, destBucket,
        skipDataDelete, next) {
            // for Azure we do not need to delete data
            if (skipDataDelete) {
                return next(null, mpuBucket, storedParts, destBucket);
            }
            // The locations were sent to metadata as an array
            // under partLocations.  Pull the partLocations.
            let locations = storedParts.map(item => item.value.partLocations);
            if (locations.length === 0) {
                return next(null, mpuBucket, storedParts, destBucket);
            }
            // flatten the array
            locations = [].concat(...locations);
            return async.eachLimit(locations, 5, (loc, cb) => {
                data.delete(loc, log, err => {
                    if (err) {
                        log.fatal('delete ObjectPart failed', { err });
                    }
                    cb();
                });
            }, () => next(null, mpuBucket, storedParts, destBucket));
        },
        function deleteMetadata(mpuBucket, storedParts, destBucket, next) {
            let splitter = constants.splitter;
            // BACKWARD: Remove to remove the old splitter
            if (mpuBucket.getMdBucketModelVersion() < 2) {
                splitter = constants.oldSplitter;
            }
            // Reconstruct mpuOverviewKey
            const mpuOverviewKey =
                `overview${splitter}${objectKey}${splitter}${uploadId}`;

            // Get the sum of all part sizes to include in pushMetric object
            const partSizeSum = storedParts.map(item => item.value.Size)
                .reduce((currPart, nextPart) => currPart + nextPart, 0);
            const keysToDelete = storedParts.map(item => item.key);
            keysToDelete.push(mpuOverviewKey);
            services.batchDeleteObjectMetadata(mpuBucket.getName(),
                keysToDelete, log, err => {
                    if (err) {
                        return next(err, destBucket);
                    }
                    pushMetric('abortMultipartUpload', log, {
                        authInfo,
                        bucket: bucketName,
                        keys: [objectKey],
                        byteLength: partSizeSum,
                    });
                    return next(null, destBucket);
                });
        },
    ], (err, destinationBucket) => {
        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, destinationBucket);
        const locationConstraint = destinationBucket ?
            destinationBucket.getLocationConstraint() : null;
        if (err === errors.NoSuchUpload) {
            log.trace('did not find valid mpu with uploadId' +
            `${uploadId}`, { method: 'multipartDelete' });
            // if legacy behavior is enabled for 'us-east-1' and
            // request is from 'us-east-1', return 404 instead of
            // 204
            if (isLegacyAWSBehavior(locationConstraint)) {
                return callback(err, corsHeaders);
            }
            // otherwise ignore error and return 204 status code
            return callback(null, corsHeaders);
        }
        return callback(err, corsHeaders);
    });
}

module.exports = multipartDelete;
