const async = require('async');
const { parseString } = require('xml2js');
const { errors, versioning, s3middleware } = require('arsenal');

const convertToXml = s3middleware.convertToXml;
const { pushMetric } = require('../utapi/utilities');
const getReplicationInfo = require('./apiUtils/object/getReplicationInfo');
const { validateAndFilterMpuParts, generateMpuPartStorageInfo } =
    require('./apiUtils/object/processMpuParts');
const { config } = require('../Config');
const multipleBackendGateway = require('../data/multipleBackendGateway');

const data = require('../data/wrapper');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const constants = require('../../constants');
const { versioningPreprocessing, checkQueryVersionId }
    = require('./apiUtils/object/versioning');
const services = require('../services');
const { metadataValidateBucketAndObj } = require('../metadata/metadataUtils');
const { skipMpuPartProcessing } = require('../data/external/utils');
const locationConstraintCheck
    = require('./apiUtils/object/locationConstraintCheck');
const locationKeysSanityCheck
    = require('./apiUtils/object/locationKeysSanityCheck');

const logger = require('../utilities/logger');

const versionIdUtils = versioning.VersionID;

let splitter = constants.splitter;
const REPLICATION_ACTION = 'MPU';

/*
   Format of xml request:
   <CompleteMultipartUpload>
     <Part>
       <PartNumber>1</PartNumber>
       <ETag>"a54357aff0632cce46d942af68356b38"</ETag>
     </Part>
     <Part>
       <PartNumber>2</PartNumber>
       <ETag>"0c78aef83f66abc1fa1e8477f296d394"</ETag>
     </Part>
     <Part>
       <PartNumber>3</PartNumber>
       <ETag>"acbd18db4cc2f85cedef654fccc4a4d8"</ETag>
     </Part>
   </CompleteMultipartUpload>
   */


  /*
  Format of xml response:
      <?xml version='1.0' encoding='UTF-8'?>
    <CompleteMultipartUploadResult
    xmlns='http://s3.amazonaws.com/doc/2006-03-01/'>
      <Location>http://Example-Bucket.s3.amazonaws.com/Example-Object</Location>
      <Bucket>Example-Bucket</Bucket>
      <Key>Example-Object</Key>
      <ETag>"3858f62230ac3c915f300c664312c11f-9"</ETag>
    </CompleteMultipartUploadResult>
   */

/**
 * completeMultipartUpload - Complete a multipart upload
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
function completeMultipartUpload(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'completeMultipartUpload' });
    const bucketName = request.bucketName;
    const objectKey = request.objectKey;
    const hostname = request.parsedHost;
    const uploadId = request.query.uploadId;
    const metadataValParams = {
        authInfo,
        bucketName,
        objectKey,
        uploadId,
        // Note: permissions for completing a multipart upload are the
        // same as putting a part.
        requestType: 'putPart or complete',
        log,
    };
    const xmlParams = {
        bucketName,
        objectKey,
        hostname,
    };
    let oldByteLength = null;

    const queryContainsVersionId = checkQueryVersionId(request.query);
    if (queryContainsVersionId instanceof Error) {
        return callback(queryContainsVersionId);
    }

    function parseXml(xmlToParse, next) {
        return parseString(xmlToParse, (err, result) => {
            if (err || !result || !result.CompleteMultipartUpload
                || !result.CompleteMultipartUpload.Part) {
                return next(errors.MalformedXML);
            }
            const jsonList = result.CompleteMultipartUpload;
            return next(null, jsonList);
        });
    }

    return async.waterfall([
        function validateDestBucket(next) {
            const metadataValParams = {
                objectKey,
                authInfo,
                bucketName,
                // Required permissions for this action
                // at the destinationBucket level are same as objectPut
                requestType: 'objectPut',
            };
            metadataValidateBucketAndObj(metadataValParams, log, next);
        },
        function validateMultipart(destBucket, objMD, next) {
            if (objMD) {
                oldByteLength = objMD['content-length'];
            }
            services.metadataValidateMultipart(metadataValParams,
                (err, mpuBucket, mpuOverview, storedMetadata) => {
                    if (err) {
                        return next(err, destBucket);
                    }
                    return next(null, destBucket, objMD, mpuBucket,
                        storedMetadata);
                });
        },
        function parsePartsList(destBucket, objMD, mpuBucket,
        storedMetadata, next) {
            const location = storedMetadata.controllingLocationConstraint;
            // BACKWARD: Remove to remove the old splitter
            if (mpuBucket.getMdBucketModelVersion() < 2) {
                splitter = constants.oldSplitter;
            }
            // Reconstruct mpuOverviewKey to point to metadata
            // originally stored when mpu initiated
            const mpuOverviewKey =
                  `overview${splitter}${objectKey}${splitter}${uploadId}`;
            if (request.post) {
                return parseXml(request.post, (err, jsonList) => {
                    if (err) {
                        return next(err, destBucket);
                    }
                    return next(null, destBucket, objMD, mpuBucket,
                        jsonList, storedMetadata, location, mpuOverviewKey);
                });
            }
            return next(errors.MalformedXML, destBucket);
        },
        function retrieveParts(destBucket, objMD, mpuBucket, jsonList,
        storedMetadata, location, mpuOverviewKey, next) {
            return services.getMPUparts(mpuBucket.getName(), uploadId, log,
                (err, result) => {
                    if (err) {
                        return next(err, destBucket);
                    }
                    const storedParts = result.Contents;
                    return next(null, destBucket, objMD, mpuBucket, storedParts,
                        jsonList, storedMetadata, location, mpuOverviewKey);
                });
        },
        function ifMultipleBackend(destBucket, objMD, mpuBucket, storedParts,
        jsonList, storedMetadata, location, mpuOverviewKey, next) {
            if (config.backends.data === 'multiple') {
                // if mpu was initiated in legacy version
                if (location === undefined) {
                    const backendInfoObj = locationConstraintCheck(request,
                        null, destBucket, log);
                    if (backendInfoObj.err) {
                        return process.nextTick(() => {
                            next(backendInfoObj.err);
                        });
                    }
                    // eslint-disable-next-line no-param-reassign
                    location = backendInfoObj.controllingLC;
                }
                const mdInfo = { storedParts, mpuOverviewKey, splitter };
                return multipleBackendGateway.completeMPU(objectKey,
                uploadId, location, jsonList, mdInfo, bucketName, null, null,
                log, (err, completeObjData) => {
                    if (err) {
                        return next(err, destBucket);
                    }
                    return next(null, destBucket, objMD, mpuBucket, storedParts,
                        jsonList, storedMetadata, completeObjData,
                        mpuOverviewKey);
                });
            }
            return next(null, destBucket, objMD, mpuBucket, storedParts,
                jsonList, storedMetadata, null, mpuOverviewKey);
        },
        function validateAndFilterParts(destBucket, objMD, mpuBucket,
        storedParts, jsonList, storedMetadata, completeObjData, mpuOverviewKey,
        next) {
            if (completeObjData) {
                return next(null, destBucket, objMD, mpuBucket, storedParts,
                jsonList, storedMetadata, completeObjData, mpuOverviewKey,
                completeObjData.filteredPartsObj);
            }
            const filteredPartsObj = validateAndFilterMpuParts(storedParts,
                jsonList, mpuOverviewKey, splitter, log);
            if (filteredPartsObj.error) {
                return next(filteredPartsObj.error, destBucket);
            }
            return next(null, destBucket, objMD, mpuBucket, storedParts,
                jsonList, storedMetadata, completeObjData, mpuOverviewKey,
                filteredPartsObj);
        },
        function processParts(destBucket, objMD, mpuBucket, storedParts,
        jsonList, storedMetadata, completeObjData, mpuOverviewKey,
        filteredPartsObj, next) {
            // if mpu was completed on backend that stored mpu MD externally,
            // skip MD processing steps
            if (completeObjData && skipMpuPartProcessing(completeObjData)) {
                const dataLocations = [
                    {
                        key: completeObjData.key,
                        size: completeObjData.contentLength,
                        start: 0,
                        dataStoreVersionId: completeObjData.dataStoreVersionId,
                        dataStoreName: storedMetadata.dataStoreName,
                        dataStoreETag: completeObjData.eTag,
                        dataStoreType: completeObjData.dataStoreType,
                    },
                ];
                const calculatedSize = completeObjData.contentLength;
                return next(null, destBucket, objMD, mpuBucket, storedMetadata,
                    completeObjData.eTag, calculatedSize, dataLocations,
                    [mpuOverviewKey], null, completeObjData);
            }

            const partsInfo =
                generateMpuPartStorageInfo(filteredPartsObj.partList);
            if (partsInfo.error) {
                return next(partsInfo.error, destBucket);
            }
            const { keysToDelete, extraPartLocations } = filteredPartsObj;
            const { aggregateETag, dataLocations, calculatedSize } = partsInfo;

            if (completeObjData) {
                const dataLocations = [
                    {
                        key: completeObjData.key,
                        size: calculatedSize,
                        start: 0,
                        dataStoreName: storedMetadata.dataStoreName,
                        dataStoreETag: aggregateETag,
                        dataStoreType: completeObjData.dataStoreType,
                    },
                ];
                return next(null, destBucket, objMD, mpuBucket, storedMetadata,
                    aggregateETag, calculatedSize, dataLocations, keysToDelete,
                    extraPartLocations, completeObjData);
            }
            return next(null, destBucket, objMD, mpuBucket, storedMetadata,
                aggregateETag, calculatedSize, dataLocations, keysToDelete,
                extraPartLocations, null);
        },
        function prepForStoring(destBucket, objMD, mpuBucket, storedMetadata,
            aggregateETag, calculatedSize, dataLocations, keysToDelete,
            extraPartLocations, completeObjData, next) {
            const metaHeaders = {};
            const keysNotNeeded =
                ['initiator', 'partLocations', 'key',
                    'initiated', 'uploadId', 'content-type', 'expires',
                    'eventualStorageBucket', 'dataStoreName'];
            const metadataKeysToPull =
                Object.keys(storedMetadata).filter(item =>
                    keysNotNeeded.indexOf(item) === -1);
            metadataKeysToPull.forEach(item => {
                metaHeaders[item] = storedMetadata[item];
            });

            const metaStoreParams = {
                authInfo,
                objectKey,
                metaHeaders,
                uploadId,
                dataStoreName: storedMetadata.dataStoreName,
                contentType: storedMetadata['content-type'],
                cacheControl: storedMetadata['cache-control'],
                contentDisposition: storedMetadata['content-disposition'],
                contentEncoding: storedMetadata['content-encoding'],
                expires: storedMetadata.expires,
                contentMD5: aggregateETag,
                size: calculatedSize,
                multipart: true,
                replicationInfo: getReplicationInfo(objectKey, destBucket,
                    false, calculatedSize, REPLICATION_ACTION),
                originOp: 's3:ObjectCreated:CompleteMultipartUpload',
                log,
            };
            if (storedMetadata['x-amz-tagging']) {
                metaStoreParams.tagging = storedMetadata['x-amz-tagging'];
            }
            if (storedMetadata.retentionMode && storedMetadata.retentionDate) {
                metaStoreParams.retentionMode = storedMetadata.retentionMode;
                metaStoreParams.retentionDate = storedMetadata.retentionDate;
            }
            if (storedMetadata.legalHold) {
                metaStoreParams.legalHold = storedMetadata.legalHold;
            }
            const serverSideEncryption =
                      destBucket.getServerSideEncryption();
            let pseudoCipherBundle = null;
            if (serverSideEncryption) {
                pseudoCipherBundle = {
                    algorithm: destBucket.getSseAlgorithm(),
                    masterKeyId: destBucket.getSseMasterKeyId(),
                };
            }
            return versioningPreprocessing(bucketName,
                destBucket, objectKey, objMD, log, (err, options) => {
                    if (err) {
                        // TODO: check AWS error when user requested a specific
                        // version before any versions have been put
                        const logLvl = err === errors.BadRequest ?
                            'debug' : 'error';
                        log[logLvl]('error getting versioning info', {
                            error: err,
                            method: 'versioningPreprocessing',
                        });
                        return next(err, destBucket);
                    }
                    const dataToDelete = options.dataToDelete;
                    metaStoreParams.versionId = options.versionId;
                    metaStoreParams.versioning = options.versioning;
                    metaStoreParams.isNull = options.isNull;
                    metaStoreParams.nullVersionId = options.nullVersionId;
                    return next(null, destBucket, dataLocations,
                        metaStoreParams, mpuBucket, keysToDelete, aggregateETag,
                        objMD, extraPartLocations, pseudoCipherBundle,
                        dataToDelete, completeObjData);
                });
        },
        function storeAsNewObj(destinationBucket, dataLocations,
            metaStoreParams, mpuBucket, keysToDelete, aggregateETag, objMD,
            extraPartLocations, pseudoCipherBundle, dataToDelete,
            completeObjData, next) {
            if (objMD) {
                // An object with the same key already exists, check
                // if it has been created by the same MPU upload by
                // checking if any of its internal location keys match
                // the new keys. In such case, it must be a duplicate
                // from a retry of a previous failed completion
                // attempt, hence do the following:
                //
                // - skip writing the new metadata key to avoid
                //   creating a new version pointing to the same data
                //   keys
                //
                // - skip old data locations deletion since the old
                //   data location keys overlap the new ones (in
                //   principle they should be fully identical as there
                //   is no reuse of previous versions' data keys in
                //   the normal process) - note that the previous
                //   failed completion attempt may have left orphan
                //   data keys but we lost track of them so we cannot
                //   delete them now
                //
                // - proceed to the deletion of overview and part
                //   metadata keys, which are likely to have failed in
                //   the previous MPU completion attempt
                //
                const onlyDifferentLocationKeys = locationKeysSanityCheck(
                    objMD.location, dataLocations);
                if (!onlyDifferentLocationKeys) {
                    log.info('MPU complete request replay detected', {
                        method: 'completeMultipartUpload.storeAsNewObj',
                        bucketName: destinationBucket.getName(),
                        objectKey: metaStoreParams.objectKey,
                        uploadId: metaStoreParams.uploadId,
                    });
                    return next(null, mpuBucket, keysToDelete, aggregateETag,
                        extraPartLocations, destinationBucket,
                        // pass the original version ID as generatedVersionId
                        objMD.versionId);
                }
            }
            return services.metadataStoreObject(destinationBucket.getName(),
                dataLocations, pseudoCipherBundle, metaStoreParams,
                (err, res) => {
                    if (err) {
                        return next(err, destinationBucket);
                    }
                    const generatedVersionId = res ? res.versionId : undefined;
                    // in cases where completing mpu overwrites a previous
                    // null version when versioning is suspended or versioning
                    // is not enabled, need to delete pre-existing data
                    // unless the preexisting object and the completed mpu
                    // are on external backends
                    if (dataToDelete) {
                        const newDataStoreName =
                            Array.isArray(dataLocations) && dataLocations[0] ?
                            dataLocations[0].dataStoreName : null;
                        const delLog =
                            logger.newRequestLoggerFromSerializedUids(log
                            .getSerializedUids());
                        return data.batchDelete(dataToDelete,
                            request.method,
                            newDataStoreName, delLog, err => {
                                if (err) {
                                    return next(err);
                                }
                                return next(null, mpuBucket, keysToDelete,
                                    aggregateETag, extraPartLocations,
                                    destinationBucket, generatedVersionId);
                            });
                    }
                    return next(null, mpuBucket, keysToDelete, aggregateETag,
                        extraPartLocations, destinationBucket,
                        generatedVersionId);
                });
        },
        function deletePartsMetadata(mpuBucket, keysToDelete, aggregateETag,
            extraPartLocations, destinationBucket, generatedVersionId, next) {
            services.batchDeleteObjectMetadata(mpuBucket.getName(),
                keysToDelete, log, err => next(err, extraPartLocations,
                    destinationBucket, aggregateETag, generatedVersionId));
        },
        function batchDeleteExtraParts(extraPartLocations, destinationBucket,
            aggregateETag, generatedVersionId, next) {
            if (extraPartLocations && extraPartLocations.length > 0) {
                const delLog = logger.newRequestLoggerFromSerializedUids(
                    log.getSerializedUids());
                return data.batchDelete(extraPartLocations, request.method,
                    null, delLog, err => {
                        if (err) {
                            return next(err);
                        }
                        return next(null, destinationBucket, aggregateETag,
                            generatedVersionId);
                    });
            }
            return next(null, destinationBucket, aggregateETag,
                generatedVersionId);
        },
    ], (err, destinationBucket, aggregateETag, generatedVersionId) => {
        const resHeaders =
            collectCorsHeaders(request.headers.origin, request.method,
                destinationBucket);
        if (err) {
            return callback(err, null, resHeaders);
        }
        if (generatedVersionId) {
            resHeaders['x-amz-version-id'] =
                versionIdUtils.encode(generatedVersionId,
                                      config.versionIdEncodingType);
        }
        xmlParams.eTag = `"${aggregateETag}"`;
        const xml = convertToXml('completeMultipartUpload', xmlParams);
        pushMetric('completeMultipartUpload', log, {
            oldByteLength,
            authInfo,
            canonicalID: destinationBucket.getOwner(),
            bucket: bucketName,
            keys: [objectKey],
            versionId: generatedVersionId,
            numberOfObjects: !generatedVersionId && oldByteLength !== null ? 0 : 1,
            location: destinationBucket.getLocationConstraint(),
        });
        return callback(null, xml, resHeaders);
    });
}

module.exports = completeMultipartUpload;
