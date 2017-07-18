const async = require('async');
const { errors, versioning, s3middleware } = require('arsenal');
const validateHeaders = s3middleware.validateConditionalHeaders;

const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const { BackendInfo } = require('./apiUtils/object/BackendInfo');
const constants = require('../../constants');
const data = require('../data/wrapper');
const kms = require('../kms/wrapper');
const metadata = require('../metadata/wrapper');
const RelayMD5Sum = require('../utilities/RelayMD5Sum');
const logger = require('../utilities/logger');
const services = require('../services');
const setUpCopyLocator = require('./apiUtils/object/setUpCopyLocator');
const { metadataValidateBucketAndObj } = require('../metadata/metadataUtils');

const versionIdUtils = versioning.VersionID;


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
                    let sourceVerId = undefined;
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
                        copyLocator.copyObjectSize, sourceVerId);
                });
        },
        // get MPU shadow bucket to get splitter based on MD version
        function getMpuShadowBucket(dataLocator, destBucketMD,
            copyObjectSize, sourceVerId, next) {
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
                        copyObjectSize, sourceVerId, splitter);
                });
        },
        // Get MPU overview object to check authorization to put a part
        // and to get any object location constraint info
        function getMpuOverviewObject(dataLocator, destBucketMD,
            copyObjectSize, sourceVerId, splitter, next) {
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
                        const objectLocationConstraint =
                            res.controllingLocationConstraint;
                        return next(null, dataLocator, destBucketMD,
                            objectLocationConstraint, copyObjectSize,
                            sourceVerId);
                    });
        },
        function goGetData(dataLocator, destBucketMD,
            objectLocationConstraint, copyObjectSize, sourceVerId, next) {
            const serverSideEncryption = destBucketMD.getServerSideEncryption();

            // skip if 0 byte object
            if (dataLocator.length === 0) {
                return process.nextTick(() => {
                    next(null, destBucketMD, [], constants.emptyFileMd5,
                        copyObjectSize, sourceVerId, serverSideEncryption);
                });
            }
            const backendInfo = new BackendInfo(objectLocationConstraint);

            // totalHash will be sent through the RelayMD5Sum transform streams
            // to collect the md5 from multiple streams
            let totalHash;
            const locations = [];
             // dataLocator is an array.  need to get and put all parts
             // in order so can get the ETag of full object
            return async.forEachOfSeries(dataLocator,
                // eslint-disable-next-line prefer-arrow-callback
                function copyPart(part, index, cb) {
                    return data.get(part, log, {}, (err, stream) => {
                        if (err) {
                            log.debug('error getting object part',
                            { error: err });
                            return cb(err);
                        }
                        const hashedStream =
                            new RelayMD5Sum(totalHash, updatedHash => {
                                totalHash = updatedHash;
                            });
                        stream.pipe(hashedStream);
                        const numberPartSize =
                            Number.parseInt(part.size, 10);
                        if (serverSideEncryption) {
                            return kms.createCipherBundle(
                                serverSideEncryption,
                                log, (err, cipherBundle) => {
                                    if (err) {
                                        log.debug('error getting cipherBundle',
                                        { error: err });
                                        return cb(errors.InternalError);
                                    }
                                    return data.put(cipherBundle, hashedStream,
                                        numberPartSize, dataStoreContext,
                                        backendInfo, log,
                                        (error, partRetrievalInfo) => {
                                            if (error) {
                                                log.debug('error putting ' +
                                                'encrypted part', { error });
                                                return cb(error);
                                            }
                                            const partResult = {
                                                key: partRetrievalInfo.key,
                                                dataStoreName: partRetrievalInfo
                                                    .dataStoreName,
                                                // Do not include part start
                                                // here since will change in
                                                // final MPU object
                                                size: part.size,
                                                sseCryptoScheme: cipherBundle
                                                    .cryptoScheme,
                                                sseCipheredDataKey: cipherBundle
                                                    .cipheredDataKey,
                                                sseAlgorithm: cipherBundle
                                                    .algorithm,
                                                sseMasterKeyId: cipherBundle
                                                    .masterKeyId,
                                            };
                                            locations.push(partResult);
                                            return cb();
                                        });
                                });
                        }
                        // Copied object is not encrypted so just put it
                        // without a cipherBundle
                        return data.put(null, hashedStream, numberPartSize,
                        dataStoreContext, backendInfo,
                        log, (error, partRetrievalInfo) => {
                            if (error) {
                                log.debug('error putting object part',
                                { error });
                                return cb(error);
                            }
                            const partResult = {
                                key: partRetrievalInfo.key,
                                dataStoreName: partRetrievalInfo.dataStoreName,
                                size: part.size,
                            };
                            locations.push(partResult);
                            return cb();
                        });
                    });
                }, err => {
                    if (err) {
                        log.debug('error transferring data from source',
                        { error: err, method: 'goGetData' });
                        return next(err, destBucketMD);
                    }
                    // Digest the final combination of all of the part streams
                    totalHash = totalHash.digest('hex');
                    return next(null, destBucketMD, locations, totalHash,
                        copyObjectSize, sourceVerId, serverSideEncryption);
                });
        },
        function getExistingPartInfo(destBucketMD, locations, totalHash,
            copyObjectSize, sourceVerId, serverSideEncryption, next) {
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
                        oldLocations);
                });
        },
        function storeNewPartMetadata(destBucketMD, locations, totalHash,
            copyObjectSize, sourceVerId, serverSideEncryption, oldLocations,
            next) {
            const lastModified = new Date().toJSON();
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
        if (err) {
            log.trace('error from copy part waterfall',
            { error: err });
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
                additionalHeaders
                ['x-amz-server-side-encryption-aws-kms-key-id'] =
                    serverSideEncryption.masterKeyId;
            }
        }
        additionalHeaders['x-amz-copy-source-version-id'] = sourceVerId;
        // TODO push metric for objectPutCopyPart
        // pushMetric('putObjectCopyPart', log, {
        //      bucket: destBucketName,
        // });
        return callback(null, xml, additionalHeaders);
    });
}

module.exports = objectPutCopyPart;
