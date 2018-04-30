const async = require('async');
const { errors, versioning, s3middleware } = require('arsenal');
const validateHeaders = s3middleware.validateConditionalHeaders;

const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const constants = require('../../constants');
const data = require('../data/wrapper');
const metadata = require('../metadata/wrapper');
const logger = require('../utilities/logger');
const services = require('../services');
const setUpCopyLocator = require('./apiUtils/object/setUpCopyLocator');
const { metadataValidateBucketAndObj } = require('../metadata/metadataUtils');
const monitoring = require('../utilities/monitoringHandler');

const versionIdUtils = versioning.VersionID;

const skipError = new Error('skip');

/**
 * PUT Part Copy during a multipart upload.
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with
 * requester's info
 * @param {request} request - request object given by router,
 *                            includes normalized headers
 * @param {string} sourceBucket - name of source bucket for object copy
 * @param {string} sourceObject - name of source object for object copy
 * @param {string} reqVersionId - versionId of the source object for copy
 * @param {object} log - the request logger
 * @param {function} callback - final callback to call with the result
 * @return {undefined}
 */
function objectPutCopyPart(authInfo, request, sourceBucket,
    sourceObject, reqVersionId, log, callback) {
    log.debug('processing request', { method: 'objectPutCopyPart' });
    const destBucketName = request.bucketName;
    const destObjectKey = request.objectKey;
    const mpuBucketName = `${constants.mpuBucketPrefix}${destBucketName}`;
    const valGetParams = {
        authInfo,
        bucketName: sourceBucket,
        objectKey: sourceObject,
        versionId: reqVersionId,
        requestType: 'objectGet',
    };

    const partNumber = Number.parseInt(request.query.partNumber, 10);
    // AWS caps partNumbers at 10,000
    if (partNumber > 10000 || !Number.isInteger(partNumber) || partNumber < 1) {
        monitoring.promMetrics('PUT', destBucketName, 400,
            'putObjectCopyPart');
        return callback(errors.InvalidArgument);
    }
    // We pad the partNumbers so that the parts will be sorted
    // in numerical order
    const paddedPartNumber = `000000${partNumber}`.substr(-5);
    // Note that keys in the query object retain their case, so
    // request.query.uploadId must be called with that exact
    // capitalization
    const uploadId = request.query.uploadId;

    const valPutParams = {
        authInfo,
        bucketName: destBucketName,
        objectKey: destObjectKey,
        requestType: 'objectPut',
    };

    // For validating the request at the MPU, the params are the same
    // as validating for the destination bucket except additionally need
    // the uploadId and splitter.
    // Also, requestType is 'putPart or complete'
    const valMPUParams = Object.assign({
        uploadId,
        splitter: constants.splitter,
    }, valPutParams);
    valMPUParams.requestType = 'putPart or complete';

    const dataStoreContext = {
        bucketName: destBucketName,
        owner: authInfo.getCanonicalID(),
        namespace: request.namespace,
        objectKey: destObjectKey,
        partNumber: paddedPartNumber,
        uploadId,
    };

    return async.waterfall([
        function checkDestAuth(next) {
            return metadataValidateBucketAndObj(valPutParams, log,
                (err, destBucketMD) => {
                    if (err) {
                        log.debug('error validating authorization for ' +
                        'destination bucket',
                        { error: err });
                        return next(err, destBucketMD);
                    }
                    const flag = destBucketMD.hasDeletedFlag()
                        || destBucketMD.hasTransientFlag();
                    if (flag) {
                        log.trace('deleted flag or transient flag ' +
                        'on destination bucket', { flag });
                        return next(errors.NoSuchBucket);
                    }
                    return next(null, destBucketMD);
                });
        },
        function checkSourceAuthorization(destBucketMD, next) {
            return metadataValidateBucketAndObj(valGetParams, log,
                (err, sourceBucketMD, sourceObjMD) => {
                    if (err) {
                        log.debug('error validating get part of request',
                            { error: err });
                        return next(err, destBucketMD);
                    }
                    if (!sourceObjMD) {
                        log.debug('no source object', { sourceObject });
                        const err = reqVersionId ? errors.NoSuchVersion :
                            errors.NoSuchKey;
                        return next(err, destBucketMD);
                    }
                    let sourceLocationConstraintName =
                        sourceObjMD.dataStoreName;
                    // for backwards compatibility before storing dataStoreName
                    // TODO: handle in objectMD class
                    if (!sourceLocationConstraintName &&
                        sourceObjMD.location[0] &&
                        sourceObjMD.location[0].dataStoreName) {
                        sourceLocationConstraintName =
                            sourceObjMD.location[0].dataStoreName;
                    }
                    if (sourceObjMD.isDeleteMarker) {
                        log.debug('delete marker on source object',
                        { sourceObject });
                        if (reqVersionId) {
                            const err = errors.InvalidRequest
                            .customizeDescription('The source of a copy ' +
                            'request may not specifically refer to a delete' +
                            'marker by version id.');
                            return next(err, destBucketMD);
                        }
                        // if user specifies a key in a versioned source bucket
                        // without specifying a version, and the object has a
                        // delete marker, return NoSuchKey
                        return next(errors.NoSuchKey, destBucketMD);
                    }
                    const headerValResult =
                        validateHeaders(request.headers,
                        sourceObjMD['last-modified'],
                        sourceObjMD['content-md5']);
                    if (headerValResult.error) {
                        return next(errors.PreconditionFailed, destBucketMD);
                    }
                    const copyLocator = setUpCopyLocator(sourceObjMD,
                        request.headers['x-amz-copy-source-range'], log);
                    if (copyLocator.error) {
                        return next(copyLocator.error, destBucketMD);
                    }
                    let sourceVerId;
                    // If specific version requested, include copy source
                    // version id in response. Include in request by default
                    // if versioning is enabled or suspended.
                    if (sourceBucketMD.getVersioningConfiguration() ||
                    reqVersionId) {
                        if (sourceObjMD.isNull || !sourceObjMD.versionId) {
                            sourceVerId = 'null';
                        } else {
                            sourceVerId =
                                versionIdUtils.encode(sourceObjMD.versionId);
                        }
                    }
                    return next(null, copyLocator.dataLocator, destBucketMD,
                        copyLocator.copyObjectSize, sourceVerId,
                        sourceLocationConstraintName);
                });
        },
        // get MPU shadow bucket to get splitter based on MD version
        function getMpuShadowBucket(dataLocator, destBucketMD,
            copyObjectSize, sourceVerId,
            sourceLocationConstraintName, next) {
            return metadata.getBucket(mpuBucketName, log,
                (err, mpuBucket) => {
                    if (err && err.NoSuchBucket) {
                        return next(errors.NoSuchUpload);
                    }
                    if (err) {
                        log.error('error getting the shadow mpu bucket', {
                            error: err,
                            method: 'objectPutCopyPart::metadata.getBucket',
                        });
                        return next(err);
                    }
                    let splitter = constants.splitter;
                    if (mpuBucket.getMdBucketModelVersion() < 2) {
                        splitter = constants.oldSplitter;
                    }
                    return next(null, dataLocator, destBucketMD,
                        copyObjectSize, sourceVerId, splitter,
                        sourceLocationConstraintName);
                });
        },
        // Get MPU overview object to check authorization to put a part
        // and to get any object location constraint info
        function getMpuOverviewObject(dataLocator, destBucketMD,
            copyObjectSize, sourceVerId, splitter,
            sourceLocationConstraintName, next) {
            const mpuOverviewKey =
                `overview${splitter}${destObjectKey}${splitter}${uploadId}`;
            return metadata.getObjectMD(mpuBucketName, mpuOverviewKey,
                    null, log, (err, res) => {
                        if (err) {
                            if (err.NoSuchKey) {
                                return next(errors.NoSuchUpload);
                            }
                            log.error('error getting overview object from ' +
                                'mpu bucket', {
                                    error: err,
                                    method: 'objectPutCopyPart::' +
                                        'metadata.getObjectMD',
                                });
                            return next(err);
                        }
                        const initiatorID = res.initiator.ID;
                        const requesterID = authInfo.isRequesterAnIAMUser() ?
                            authInfo.getArn() : authInfo.getCanonicalID();
                        if (initiatorID !== requesterID) {
                            return next(errors.AccessDenied);
                        }
                        const destObjLocationConstraint =
                            res.controllingLocationConstraint;
                        return next(null, dataLocator, destBucketMD,
                            destObjLocationConstraint, copyObjectSize,
                            sourceVerId, sourceLocationConstraintName);
                    });
        },
        function goGetData(dataLocator, destBucketMD,
            destObjLocationConstraint, copyObjectSize, sourceVerId,
            sourceLocationConstraintName, next) {
            data.uploadPartCopy(request, log, destBucketMD,
            sourceLocationConstraintName,
            destObjLocationConstraint, dataLocator, dataStoreContext,
            (error, eTag, lastModified, serverSideEncryption, locations) => {
                if (error) {
                    if (error.message === 'skip') {
                        return next(skipError, destBucketMD, eTag,
                            lastModified, sourceVerId,
                            serverSideEncryption);
                    }
                    return next(error, destBucketMD);
                }
                return next(null, destBucketMD, locations, eTag,
                copyObjectSize, sourceVerId, serverSideEncryption,
                lastModified);
            });
        },
        function getExistingPartInfo(destBucketMD, locations, totalHash,
            copyObjectSize, sourceVerId, serverSideEncryption, lastModified,
            next) {
            const partKey =
                `${uploadId}${constants.splitter}${paddedPartNumber}`;
            metadata.getObjectMD(mpuBucketName, partKey, {}, log,
                (err, result) => {
                    // If there is nothing being overwritten just move on
                    if (err && !err.NoSuchKey) {
                        log.debug('error getting current part (if any)',
                        { error: err });
                        return next(err);
                    }
                    let oldLocations;
                    if (result) {
                        oldLocations = result.partLocations;
                        // Pull locations to clean up any potential orphans
                        // in data if object put is an overwrite of
                        // already existing object with same key and part number
                        oldLocations = Array.isArray(oldLocations) ?
                            oldLocations : [oldLocations];
                    }
                    return next(null, destBucketMD, locations, totalHash,
                        copyObjectSize, sourceVerId, serverSideEncryption,
                        lastModified, oldLocations);
                });
        },
        function storeNewPartMetadata(destBucketMD, locations, totalHash,
            copyObjectSize, sourceVerId, serverSideEncryption, lastModified,
            oldLocations, next) {
            const metaStoreParams = {
                partNumber: paddedPartNumber,
                contentMD5: totalHash,
                size: copyObjectSize,
                uploadId,
                splitter: constants.splitter,
                lastModified,
            };
            return services.metadataStorePart(mpuBucketName,
                locations, metaStoreParams, log, err => {
                    if (err) {
                        log.debug('error storing new metadata',
                        { error: err, method: 'storeNewPartMetadata' });
                        return next(err);
                    }
                    // Clean up the old data now that new metadata (with new
                    // data locations) has been stored
                    if (oldLocations) {
                        data.batchDelete(oldLocations, request.method, null,
                            logger.newRequestLoggerFromSerializedUids(
                                log.getSerializedUids()));
                    }
                    return next(null, destBucketMD, totalHash, lastModified,
                        sourceVerId, serverSideEncryption);
                });
        },
    ], (err, destBucketMD, totalHash, lastModified, sourceVerId,
        serverSideEncryption) => {
        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, destBucketMD);
        if (err && err !== skipError) {
            log.trace('error from copy part waterfall',
            { error: err });
            monitoring.promMetrics('PUT', destBucketName, err.code,
                'putObjectCopyPart');
            return callback(err, null, corsHeaders);
        }
        const xml = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<CopyPartResult>',
            '<LastModified>', new Date(lastModified)
                .toISOString(), '</LastModified>',
            '<ETag>&quot;', totalHash, '&quot;</ETag>',
            '</CopyPartResult>',
        ].join('');

        const additionalHeaders = corsHeaders || {};
        if (serverSideEncryption) {
            additionalHeaders['x-amz-server-side-encryption'] =
                serverSideEncryption.algorithm;
            if (serverSideEncryption.algorithm === 'aws:kms') {
                additionalHeaders['x-amz-server-side-encryption-aws-kms-key-id']
                    = serverSideEncryption.masterKeyId;
            }
        }
        additionalHeaders['x-amz-copy-source-version-id'] = sourceVerId;
        // TODO push metric for objectPutCopyPart
        // pushMetric('putObjectCopyPart', log, {
        //      bucket: destBucketName,
        //      keys: [objectKey],
        // });
        monitoring.promMetrics(
            'PUT', destBucketName, '200', 'putObjectCopyPart');
        return callback(null, xml, additionalHeaders);
    });
}

module.exports = objectPutCopyPart;
