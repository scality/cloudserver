const async = require('async');

const constants = require('../../../../constants');
const { data } = require('../../../data/wrapper');
const locationConstraintCheck = require('../object/locationConstraintCheck');
const { standardMetadataValidateBucketAndObj } =
    require('../../../metadata/metadataUtils');
const { validateQuotas } = require('../quotas/quotaUtils');
const services = require('../../../services');
const metadata = require('../../../metadata/wrapper');

function abortMultipartUpload(authInfo, bucketName, objectKey, uploadId, log,
    callback, request) {
    const metadataValMPUparams = {
        authInfo,
        bucketName,
        objectKey,
        uploadId,
        preciseRequestType: request.apiMethods || 'multipartDelete',
        request,
    };

    log.debug('processing request', { method: 'abortMultipartUpload' });
    // For validating the request at the destinationBucket level
    // params are the same as validating at the MPU level
    // but the requestType is the more general 'objectDelete'
    const metadataValParams = Object.assign({}, metadataValMPUparams);
    metadataValParams.requestType = 'objectPut';
    const authzIdentityResult = request ? request.actionImplicitDenies : false;

    async.waterfall([
        function checkDestBucketVal(next) {
            standardMetadataValidateBucketAndObj(metadataValParams, authzIdentityResult, log,
                (err, destinationBucket, objectMD) => {
                    if (err) {
                        log.error('error validating request', { error: err });
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
                    return next(null, destinationBucket, objectMD);
                });
        },
        function checkMPUval(destBucket, objectMD, next) {
            metadataValParams.log = log;
            services.metadataValidateMultipart(metadataValParams,
                (err, mpuBucket, mpuOverviewObj) => {
                    if (err) {
                        log.error('error validating multipart', { error: err });
                        return next(err, destBucket);
                    }
                    return next(err, mpuBucket, mpuOverviewObj, destBucket, objectMD);
                });
        },
        function abortExternalMpu(mpuBucket, mpuOverviewObj, destBucket, objectMD,
        next) {
            const location = mpuOverviewObj.controllingLocationConstraint;
            const originalIdentityAuthzResults = request.actionImplicitDenies;
            // eslint-disable-next-line no-param-reassign
            delete request.actionImplicitDenies;
            return data.abortMPU(objectKey, uploadId, location, bucketName,
            request, destBucket, locationConstraintCheck, log,
            (err, skipDataDelete) => {
                // eslint-disable-next-line no-param-reassign
                request.actionImplicitDenies = originalIdentityAuthzResults;
                if (err) {
                    log.error('error aborting MPU', { error: err });
                    return next(err, destBucket);
                }
                // for Azure and GCP we do not need to delete data
                // for all other backends, skipDataDelete will be set to false
                return next(null, mpuBucket, destBucket, objectMD, skipDataDelete);
            });
        },
        function getPartLocations(mpuBucket, destBucket, objectMD, skipDataDelete,
        next) {
            services.getMPUparts(mpuBucket.getName(), uploadId, log,
                (err, result) => {
                    if (err) {
                        log.error('error getting parts', { error: err });
                        return next(err, destBucket);
                    }
                    const storedParts = result.Contents;
                    return next(null, mpuBucket, storedParts, destBucket, objectMD,
                    skipDataDelete);
                });
        },
        function deleteObjectMetadata(mpuBucket, storedParts, destBucket, objectMD, skipDataDelete, next) {
            if (!objectMD || metadataValMPUparams.uploadId !== objectMD.uploadId) {
                return next(null, mpuBucket, storedParts, destBucket, objectMD, skipDataDelete);
            }
            // In case there has been an error during cleanup after a complete MPU
            // (e.g. failure to delete MPU MD in shadow bucket),
            // we need to ensure that the MPU metadata is deleted.
            log.debug('Object has existing metadata, deleting them', {
                method: 'abortMultipartUpload',
                bucketName,
                objectKey,
                uploadId,
                versionId: objectMD.versionId,
            });
            return metadata.deleteObjectMD(bucketName, objectKey, { versionId: objectMD.versionId }, log, err => {
                if (err) {
                    log.error('error deleting object metadata', { error: err });
                }
                return next(err, mpuBucket, storedParts, destBucket, objectMD, skipDataDelete);
            });
        },
        function deleteData(mpuBucket, storedParts, destBucket, objectMD,
        skipDataDelete, next) {
            if (skipDataDelete) {
                return next(null, mpuBucket, storedParts, destBucket);
            }
            // The locations were sent to metadata as an array
            // under partLocations.  Pull the partLocations.
            const locations = storedParts.flatMap(item => item.value.partLocations);
            if (locations.length === 0) {
                return next(null, mpuBucket, storedParts, destBucket);
            }

            if (objectMD?.location) {
                const existingLocations = new Set(locations.map(loc => loc.key));
                const remainingObjectLocations = objectMD.location.filter(loc => !existingLocations.has(loc.key));
                locations.push(...remainingObjectLocations);
            }

            return async.eachLimit(locations, 5, (loc, cb) => {
                data.delete(loc, log, err => {
                    if (err) {
                        log.fatal('delete ObjectPart failed', { err });
                    }
                    cb();
                });
            }, () => {
                const length = storedParts.reduce((length, loc) => length + loc.value.Size, 0);
                return validateQuotas(request, destBucket, request.accountQuotas,
                    ['objectDelete'], 'objectDelete', -length, false, log, err => {
                        if (err) {
                            // Ignore error, as the data has been deleted already: only inflight count
                            // has not been updated, and will be eventually consistent anyway
                            log.warn('failed to update inflights', {
                                method: 'abortMultipartUpload',
                                locations,
                                error: err,
                            });
                        }
                        next(null, mpuBucket, storedParts, destBucket);
                    });
            });
        },
        function deleteShadowObjectMetadata(mpuBucket, storedParts, destBucket, next) {
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
