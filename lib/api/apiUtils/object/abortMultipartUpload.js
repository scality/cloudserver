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
    };
    // For validating the request at the destinationBucket level
    // params are the same as validating at the MPU level
    // but the requestType is the more general 'objectDelete'
    const metadataValParams = Object.assign({}, metadataValMPUparams);
    metadataValParams.requestType = 'objectPut';

    async.waterfall([
        function checkDestBucketVal(next) {
            metadataValidateBucketAndObj(metadataValParams, log,
                (err, destinationBucket, objMD) => {
                    if (err) {
                        return next(err, destinationBucket, 0, objMD);
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
                    return next(null, destinationBucket, objMD);
                });
        },
        function checkMPUval(destBucket, objMD, next) {
            metadataValParams.log = log;
            services.metadataValidateMultipart(metadataValParams,
                (err, mpuBucket, mpuOverviewObj) => {
                    if (err) {
                        return next(err, destBucket, 0, objMD);
                    }
                    return next(err, mpuBucket, mpuOverviewObj, destBucket,
                        objMD);
                });
        },
        function ifMultipleBackend(mpuBucket, mpuOverviewObj, destBucket,
        objMD, next) {
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
                            next(backendInfoObj.err, destBucket, 0, objMD);
                        });
                    }
                    location = backendInfoObj.controllingLC;
                } else {
                    location = mpuOverviewObj.controllingLocationConstraint;
                }
                return multipleBackendGateway.abortMPU(objectKey, uploadId,
                location, bucketName, log, (err, skipDataDelete) => {
                    if (err) {
                        return next(err, destBucket, 0, objMD);
                    }
                    return next(null, mpuBucket, destBucket,
                    skipDataDelete, objMD);
                });
            }
            return next(null, mpuBucket, destBucket, false, objMD);
        },
        function getPartLocations(mpuBucket, destBucket, skipDataDelete,
        objMD, next) {
            services.getMPUparts(mpuBucket.getName(), uploadId, log,
                (err, result) => {
                    if (err) {
                        return next(err, destBucket, 0, objMD);
                    }
                    const storedParts = result.Contents;
                    return next(null, mpuBucket, storedParts, destBucket,
                    skipDataDelete, objMD);
                });
        },
        function deleteData(mpuBucket, storedParts, destBucket,
        skipDataDelete, objMD, next) {
            // for Azure we do not need to delete data
            if (skipDataDelete) {
                return next(null, mpuBucket, storedParts, destBucket, objMD);
            }
            // The locations were sent to metadata as an array
            // under partLocations.  Pull the partLocations.
            let locations = storedParts.map(item => item.value.partLocations);
            if (locations.length === 0) {
                return next(null, mpuBucket, storedParts, destBucket, objMD);
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
            }, () => next(null, mpuBucket, storedParts, destBucket, objMD));
        },
        function deleteMetadata(mpuBucket, storedParts, destBucket, objMD,
        next) {
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
                keysToDelete, log, err => next(err, destBucket, partSizeSum,
                    objMD));
        },
    ], callback);
}

module.exports = abortMultipartUpload;
