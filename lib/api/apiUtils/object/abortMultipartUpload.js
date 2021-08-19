const async = require('async');

const { config } = require('../../../Config');
const constants = require('../../../../constants');
const data = require('../../../data/wrapper');
const locationConstraintCheck = require('../object/locationConstraintCheck');
const { metadataValidateBucketAndObj } =
    require('../../../metadata/metadataUtils');
const multipleBackendGateway = require('../../../data/multipleBackendGateway');
const services = require('../../../services');

function abortMultipartUpload(authInfo, bucketName, objectKey, uploadId, log,
    callback, request) {
    const metadataValMPUparams = {
        authInfo,
        bucketName,
        objectKey,
        uploadId,
        preciseRequestType: 'multipartDelete',
        request,
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
                let location;
                // if controlling location constraint is not stored in object
                // metadata, mpu was initiated in legacy S3C, so need to
                // determine correct location constraint
                if (!mpuOverviewObj.controllingLocationConstraint) {
                    const backendInfoObj = locationConstraintCheck(request,
                        null, destBucket, log);
                    if (backendInfoObj.err) {
                        return process.nextTick(() => {
                            next(backendInfoObj.err, destBucket);
                        });
                    }
                    location = backendInfoObj.controllingLC;
                } else {
                    location = mpuOverviewObj.controllingLocationConstraint;
                }
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
                keysToDelete, log, err => next(err, destBucket, partSizeSum));
        },
    ], callback);
}

module.exports = abortMultipartUpload;
