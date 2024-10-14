const async = require('async');
const { parseString } = require('xml2js');
const { errors, versioning, s3middleware, storage } = require('arsenal');

const convertToXml = s3middleware.convertToXml;
const { pushMetric } = require('../utapi/utilities');
const getReplicationInfo = require('./apiUtils/object/getReplicationInfo');

const { data } = require('../data/wrapper');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const constants = require('../../constants');
const { versioningPreprocessing, checkQueryVersionId, decodeVID, overwritingVersioning }
    = require('./apiUtils/object/versioning');
const services = require('../services');
const { standardMetadataValidateBucketAndObj } = require('../metadata/metadataUtils');
const locationConstraintCheck
    = require('./apiUtils/object/locationConstraintCheck');
const { skipMpuPartProcessing } = storage.data.external.backendUtils;
const { validateAndFilterMpuParts, generateMpuPartStorageInfo } =
    s3middleware.processMpuParts;
const locationKeysHaveChanged
    = require('./apiUtils/object/locationKeysHaveChanged');
const { setExpirationHeaders } = require('./apiUtils/object/expirationHeaders');
const { validatePutVersionId } = require('./apiUtils/object/coldStorage');
const { VersionID } = require('arsenal/build/lib/versioning');

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
        requestType: request.apiMethods || 'putPart or complete',
        log,
        request,
    };
    const xmlParams = {
        bucketName,
        objectKey,
        hostname,
    };
    let oldByteLength = null;
    const responseHeaders = {};

    let versionId;
    const putVersionId = request.headers['x-scal-s3-version-id'];
    const isPutVersion = putVersionId || putVersionId === '';
    if (putVersionId) {
        const decodedVidResult = decodeVID(putVersionId);
        if (decodedVidResult instanceof Error) {
            log.trace('invalid x-scal-s3-version-id header', {
                versionId: putVersionId,
                error: decodedVidResult,
            });
            return process.nextTick(() => callback(decodedVidResult));
        }
        versionId = decodedVidResult;
    }

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
                requestType: request.apiMethods || 'completeMultipartUpload',
                versionId,
                request,
            };
            standardMetadataValidateBucketAndObj(metadataValParams, request.actionImplicitDenies, log, next);
        },
        function validateMultipart(destBucket, objMD, next) {
            if (objMD) {
                oldByteLength = objMD['content-length'];
            }

            if (isPutVersion) {
                const error = validatePutVersionId(objMD, putVersionId, log);
                if (error) {
                    return next(error, destBucket);
                }
            }

            return services.metadataValidateMultipart(metadataValParams,
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
        function markOverviewForCompletion(destBucket, objMD, mpuBucket, jsonList,
        storedMetadata, location, mpuOverviewKey, next) {
            return services.metadataMarkMPObjectForCompletion({
                bucketName: mpuBucket.getName(),
                objectKey,
                uploadId,
                splitter,
                storedMetadata,
            }, log, err => {
                if (err) {
                    return next(err);
                }
                return next(null, destBucket, objMD, mpuBucket,
                            jsonList, storedMetadata, location, mpuOverviewKey);
            });
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
        function completeExternalMpu(destBucket, objMD, mpuBucket, storedParts,
        jsonList, storedMetadata, location, mpuOverviewKey, next) {
            const mdInfo = { storedParts, mpuOverviewKey, splitter };
            const mpuInfo =
                { objectKey, uploadId, jsonList, bucketName, destBucket };
            const originalIdentityImpDenies = request.actionImplicitDenies;
            // eslint-disable-next-line no-param-reassign
            delete request.actionImplicitDenies;
            return data.completeMPU(request, mpuInfo, mdInfo, location,
            null, null, null, locationConstraintCheck, log,
            (err, completeObjData) => {
                // eslint-disable-next-line no-param-reassign
                request.actionImplicitDenies = originalIdentityImpDenies;
                if (err) {
                    return next(err, destBucket);
                }
                // if mpu not handled externally, completeObjData will be null
                return next(null, destBucket, objMD, mpuBucket, storedParts,
                    jsonList, storedMetadata, completeObjData, mpuOverviewKey);
            });
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
                isDeleteMarker: false,
                replicationInfo: getReplicationInfo(objectKey, destBucket,
                    false, calculatedSize, REPLICATION_ACTION),
                originOp: 's3:ObjectCreated:CompleteMultipartUpload',
                overheadField: constants.overheadField,
                log,
            };
            // If key already exists
            if (objMD) {
                // Re-use creation-time if we can
                if (objMD['creation-time']) {
                    metaStoreParams.creationTime = objMD['creation-time'];
                // Otherwise fallback to last-modified
                } else {
                    metaStoreParams.creationTime = objMD['last-modified'];
                }
            // If its a new key, create a new timestamp
            } else {
                metaStoreParams.creationTime = new Date().toJSON();
            }
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
            // if x-scal-s3-version-id header is specified, we overwrite the object/version metadata.
            if (isPutVersion) {
                const options = overwritingVersioning(objMD, metaStoreParams);
                return process.nextTick(() => next(null, destBucket, dataLocations,
                    metaStoreParams, mpuBucket, keysToDelete, aggregateETag,
                    objMD, extraPartLocations, pseudoCipherBundle,
                    completeObjData, options));
            }

            if (!destBucket.isVersioningEnabled() && objMD?.archive?.archiveInfo) {
                // Ensure we trigger a "delete" event in the oplog for the previously archived object
                metaStoreParams.needOplogUpdate = 's3:ReplaceArchivedObject';
            }

            return versioningPreprocessing(bucketName,
                destBucket, objectKey, objMD, log, (err, options) => {
                    if (err) {
                        // TODO: check AWS error when user requested a specific
                        // version before any versions have been put
                        const logLvl = err.is.BadRequest ? 'debug' : 'error';
                        log[logLvl]('error getting versioning info', {
                            error: err,
                            method: 'versioningPreprocessing',
                        });
                        return next(err, destBucket);
                    }
                    return next(null, destBucket, dataLocations,
                        metaStoreParams, mpuBucket, keysToDelete, aggregateETag,
                        objMD, extraPartLocations, pseudoCipherBundle,
                        completeObjData, options);
                });
        },
        function storeAsNewObj(destinationBucket, dataLocations,
            metaStoreParams, mpuBucket, keysToDelete, aggregateETag, objMD,
            extraPartLocations, pseudoCipherBundle,
            completeObjData, options, next) {
            const dataToDelete = options.dataToDelete;
            const location = dataLocations[0].dataStoreName;
            /* eslint-disable no-param-reassign */
            if (location === destinationBucket.getLocationConstraint() && destinationBucket.isIngestionBucket()) {
                // If the object is being written to the "ingested" storage location, keep the same
                // versionId for consistency and to avoid creating an extra version when it gets
                // ingested
                metaStoreParams.versionId = VersionID.decode(dataLocations[0].dataStoreVersionId);
            } else {
                metaStoreParams.versionId = options.versionId;
            }
            metaStoreParams.versioning = options.versioning;
            metaStoreParams.isNull = options.isNull;
            metaStoreParams.deleteNullKey = options.deleteNullKey;
            if (options.extraMD) {
                Object.assign(metaStoreParams, options.extraMD);
            }
            /* eslint-enable no-param-reassign */

            // For external backends (where completeObjData is not
            // null), the backend key does not change for new versions
            // of the same object (or rewrites for nonversioned
            // buckets), hence the deduplication sanity check does not
            // make sense for external backends.
            if (objMD && !completeObjData) {
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
                if (!locationKeysHaveChanged(objMD.location, dataLocations)) {
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

                    setExpirationHeaders(responseHeaders, {
                        lifecycleConfig: destinationBucket.getLifecycleConfiguration(),
                        objectParams: {
                            key: objectKey,
                            date: res.lastModified,
                            tags: res.tags,
                        },
                    });

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
                        return data.batchDelete(dataToDelete,
                            request.method,
                            newDataStoreName, log, err => {
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
                return data.batchDelete(extraPartLocations, request.method,
                    null, log, err => {
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
        const corsHeaders =
            collectCorsHeaders(request.headers.origin, request.method,
                destinationBucket);
        if (err) {
            return callback(err, null, corsHeaders);
        }
        if (generatedVersionId) {
            corsHeaders['x-amz-version-id'] =
                versionIdUtils.encode(generatedVersionId);
        }
        Object.assign(responseHeaders, corsHeaders);

        const vcfg = destinationBucket.getVersioningConfiguration();
        const isVersionedObj = vcfg && vcfg.Status === 'Enabled';

        xmlParams.eTag = `"${aggregateETag}"`;
        const xml = convertToXml('completeMultipartUpload', xmlParams);
        pushMetric('completeMultipartUpload', log, {
            oldByteLength: isVersionedObj ? null : oldByteLength,
            authInfo,
            canonicalID: destinationBucket.getOwner(),
            bucket: bucketName,
            keys: [objectKey],
            versionId: generatedVersionId,
            numberOfObjects: !generatedVersionId && oldByteLength !== null ? 0 : 1,
            location: destinationBucket.getLocationConstraint(),
        });
        return callback(null, xml, responseHeaders);
    });
}

module.exports = completeMultipartUpload;
