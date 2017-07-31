const crypto = require('crypto');
const async = require('async');
const { parseString } = require('xml2js');
const { errors, versioning, s3middleware } = require('arsenal');

const escapeForXml = s3middleware.escapeForXml;
const { pushMetric } = require('../utapi/utilities');
const getReplicationInfo = require('./apiUtils/object/getReplicationInfo');
const { config } = require('../Config');
const multipleBackendGateway = require('../data/multipleBackendGateway');

const data = require('../data/wrapper');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const constants = require('../../constants');
const { versioningPreprocessing, checkQueryVersionId }
    = require('./apiUtils/object/versioning');
const metadata = require('../metadata/wrapper');
const services = require('../services');
const { metadataValidateBucketAndObj } = require('../metadata/metadataUtils');

const logger = require('../utilities/logger');

const versionIdUtils = versioning.VersionID;

let splitter = constants.splitter;

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
 * _convertToXml - Convert the `xmlParams` object created in
 * `completeMultipartUpload()` to an XML DOM string
 * @param {object} xmlParams - The object created in
 * `completeMultipartUpload()` to convert into an XML DOM string
 * @return {string} xml.join('') - The XML DOM string
 */
const _convertToXml = xmlParams => {
    const xml = [];

    xml.push('<?xml version="1.0" encoding="UTF-8"?>',
             '<CompleteMultipartUploadResult ' +
                'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
             `<Location>http://${xmlParams.bucketName}.` +
                `${escapeForXml(xmlParams.hostname)}/` +
                `${escapeForXml(xmlParams.objectKey)}</Location>`,
             `<Bucket>${xmlParams.bucketName}</Bucket>`,
             `<Key>${escapeForXml(xmlParams.objectKey)}</Key>`,
             `<ETag>${xmlParams.ETag}</ETag>`,
             '</CompleteMultipartUploadResult>'
    );

    return xml.join('');
};


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
            services.metadataValidateMultipart(metadataValParams,
                (err, mpuBucket) => {
                    if (err) {
                        return next(err, destBucket);
                    }
                    return next(null, destBucket, objMD, mpuBucket);
                });
        },
        function parsePartsList(destBucket, objMD, mpuBucket, next) {
            if (request.post) {
                return parseXml(request.post, (err, jsonList) => {
                    if (err) {
                        return next(err, destBucket);
                    }
                    return next(err, destBucket, objMD, mpuBucket, jsonList);
                });
            }
            return next(errors.MalformedXML, destBucket);
        },
        function getMPUMetadata(destBucket, objMD, mpuBucket, jsonList, next) {
            // BACKWARD: Remove to remove the old splitter
            if (mpuBucket.getMdBucketModelVersion() < 2) {
                splitter = constants.oldSplitter;
            }
            // Reconstruct mpuOverviewKey to serve
            // as key to pull metadata originally stored when mpu initiated
            const mpuOverviewKey =
                `overview${splitter}${objectKey}${splitter}${uploadId}`;

            return metadata.getObjectMD(mpuBucket.getName(), mpuOverviewKey,
            {}, log, (err, storedMetadata) => {
                if (err) {
                    return next(err, destBucket);
                }
                const location = storedMetadata.controllingLocationConstraint;
                return next(null, destBucket, objMD, mpuBucket, jsonList,
                storedMetadata, location, mpuOverviewKey);
            });
        },
        function ifMultipleBackend(destBucket, objMD, mpuBucket, jsonList,
        storedMetadata, location, mpuOverviewKey, next) {
            if (config.backends.data === 'multiple') {
                return multipleBackendGateway.completeMPU(objectKey,
                uploadId, location, jsonList, destBucket, log,
                (err, completeObjData) => {
                    if (err) {
                        return next(err, destBucket);
                    }
                    return next(null, destBucket, objMD, mpuBucket,
                    jsonList, storedMetadata, completeObjData, mpuOverviewKey);
                });
            }
            return next(null, destBucket, objMD, mpuBucket, jsonList,
            storedMetadata, null, mpuOverviewKey);
        },
        function retrieveParts(destBucket, objMD, mpuBucket, jsonList,
        storedMetadata, completeObjData, mpuOverviewKey, next) {
            // means MPU completion was done on third-party backend,
            // so part info is not in our metadata and related steps
            // should be skipped
            if (completeObjData) {
                return next(null, destBucket, objMD, mpuBucket, null,
                jsonList, storedMetadata, completeObjData, mpuOverviewKey);
            }
            return services.getMPUparts(mpuBucket.getName(), uploadId, log,
                (err, result) => {
                    if (err) {
                        return next(err, destBucket);
                    }
                    const storedParts = result.Contents;
                    return next(null, destBucket, objMD, mpuBucket,
                        storedParts, jsonList, storedMetadata, null,
                        mpuOverviewKey);
                });
        },
        function processParts(destBucket, objMD, mpuBucket, storedParts,
            jsonList, storedMetadata, completeObjData, mpuOverviewKey, next) {
            let calculatedSize = 0;
            if (completeObjData) {
                calculatedSize = completeObjData.contentLength;
                return next(null, destBucket, objMD, mpuBucket,
                    storedMetadata, completeObjData.eTag, calculatedSize,
                    null, [mpuOverviewKey], null, completeObjData);
            }
            let storedPartsAsObjects = [];
            const keysToDelete = [];
            storedParts.forEach(item => {
                keysToDelete.push(item.key);
                storedPartsAsObjects.push({
                    // In order to delete the part listing in the shadow
                    // bucket, need the full key
                    key: item.key,
                    ETag: `"${item.value.ETag}"`,
                    size: item.value.Size,

                    locations: Array.isArray(item.value.partLocations) ?
                        item.value.partLocations : [item.value.partLocations],
                });
            });
            keysToDelete.push(mpuOverviewKey);
            const dataLocations = [];
            // AWS documentation is unclear on what the MD5 is that it returns
            // in the response for a complete multipart upload request.
            // The docs state that they might or might not
            // return the MD5 of the complete object. It appears
            // they are returning the MD5 of the parts' MD5s so that is
            // what we have done here. We:
            // 1) concatenate the hex version of the
            // individual ETags
            // 2) convert the concatenated hex to binary
            // 3) take the md5 of the binary
            // 4) create the hex digest of the md5
            // 5) add '-' plus the number of parts at the end
            let concatETags = '';
            // Check list sent to make sure valid
            // If the number of the parts in the JSON does not
            // match the number of parts stored, return error msg
            const partLength = jsonList.Part.length;

            // A user can put more parts than they end up including
            // in the completed MPU but there cannot be more
            // parts in the complete message than were already put
            if (partLength > storedPartsAsObjects.length) {
                return next(errors.InvalidPart, destBucket);
            }

            let extraParts = [];
            const extraPartLocations = [];

            for (let i = 0; i < partLength; i++) {
                const part = jsonList.Part[i];
                // If the complete list of parts sent with
                // the complete multipart upload request is not
                // in ascending order return an error
                const partNumber = Number.parseInt(part.PartNumber[0], 10);
                if (i > 0) {
                    const previousPartNumber =
                      Number.parseInt(jsonList.Part[i - 1].PartNumber[0], 10);
                    if (partNumber <= previousPartNumber) {
                        return next(errors.InvalidPartOrder, destBucket);
                    }
                }

                let isPartUploaded = false;
                while (storedPartsAsObjects.length > 0 && !isPartUploaded) {
                    const storedPart = storedPartsAsObjects[0];
                    const partNumberUploaded =
                      Number.parseInt(storedPart.key.split(splitter)[1], 10);
                    if (partNumberUploaded === partNumber) {
                        isPartUploaded = true;
                        // some clients send base64, convert to hex
                        // 32 chars = 16 bytes(2 chars-per-byte) = 128 bits of
                        // MD5 hex
                        let partETag = part.ETag[0].replace(/['"]/g, '');
                        if (partETag.length !== 32) {
                            const buffered = Buffer.from(part.ETag[0], 'base64')
                                .toString('hex');
                            partETag = `${buffered}`;
                        }
                        const dataStoreETag = `${i + 1}:${partETag}`;
                        partETag = `"${partETag}"`;
                        // If the list of parts sent with
                        // the complete multipartupload request contains
                        // a part ETag that does not match
                        // the ETag for the part already sent, return an error
                        if (partETag !== storedPart.ETag) {
                            return next(errors.InvalidPart, destBucket);
                        }
                        // If any part other than the last part is less than
                        // 5MB, return an error
                        const partSize = Number.parseInt(storedPart.size, 10);

                        // allow smaller parts for testing
                        if (process.env.MPU_TESTING) {
                            log.info('MPU_TESTING env variable setting',
                                { setting: process.env.MPU_TESTING });
                        }
                        if (process.env.MPU_TESTING !== 'yes' &&
                            i < jsonList.Part.length - 1 &&
                            partSize < constants.minimumAllowedPartSize) {
                            log.debug('part too small on complete mpu');
                            return next(errors.EntityTooSmall, destBucket);
                        }
                        // Assemble array of part locations, aggregate size
                        // and build string to create aggregate ETag

                        // If part was put by a regular put part rather than a
                        // copy it is always one location.  With a put part
                        // copy, could be multiple locations so loop over array
                        // of locations.
                        for (let j = 0; j < storedPart.locations.length;
                            j ++) {
                            // If the piece has parts (was a put part object
                            // copy) each piece will have a size attribute.
                            // Otherwise, the piece was put by a regular put
                            // part and the size the of the piece is the full
                            // part size.
                            const location = storedPart.locations[j];
                            // If there is no location, move on
                            if (!location || typeof location !== 'object') {
                                continue;
                            }
                            let pieceSize = partSize;
                            if (location.size) {
                                pieceSize = Number.parseInt(location.size, 10);
                            }
                            const pieceRetrievalInfo = {
                                key: location.key,
                                size: pieceSize,
                                start: calculatedSize,
                                dataStoreName: location.dataStoreName,
                                dataStoreETag,
                                cryptoScheme: location.sseCryptoScheme,
                                cipheredDataKey: location.sseCipheredDataKey,
                            };
                            dataLocations.push(pieceRetrievalInfo);
                            calculatedSize += pieceSize;
                        }

                        const partETagWithoutQuotes =
                            storedPart.ETag.slice(1, -1);
                        concatETags += partETagWithoutQuotes;
                        storedPartsAsObjects =
                          storedPartsAsObjects.splice(1);
                    } else {
                        extraParts.push(storedPart);
                        storedPartsAsObjects =
                          storedPartsAsObjects.splice(1);
                    }
                }
                if (!isPartUploaded) {
                    return next(errors.InvalidPart, destBucket);
                }
            }
            extraParts = extraParts.concat(storedPartsAsObjects);
            // if extra parts, need to delete the data when done with completing
            // mpu so extract the info to delete here
            if (extraParts.length > 0) {
                extraParts.forEach(part => {
                    const locations = part.locations;
                    locations.forEach(location => {
                        if (!location || typeof location !== 'object') {
                            return;
                        }
                        extraPartLocations.push(location);
                    });
                });
            }
            // Convert the concatenated hex ETags to binary
            const bufferedHex = Buffer.from(concatETags, 'hex');
            // Convert the buffer to a binary string
            const binaryString = bufferedHex.toString('binary');
            // Get the md5 of the binary string
            const md5Hash = crypto.createHash('md5');
            md5Hash.update(binaryString, 'binary');
            // Get the hex digest of the md5
            let aggregateETag = md5Hash.digest('hex');
            // Add the number of parts at the end
            aggregateETag = `${aggregateETag}-${jsonList.Part.length}`;
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
                'eventualStorageBucket'];
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
                contentType: storedMetadata['content-type'],
                cacheControl: storedMetadata['cache-control'],
                contentDisposition: storedMetadata['content-disposition'],
                contentEncoding: storedMetadata['content-encoding'],
                expires: storedMetadata.expires,
                contentMD5: aggregateETag,
                size: calculatedSize,
                multipart: true,
                replicationInfo: getReplicationInfo(objectKey, destBucket,
                    false, calculatedSize),
                log,
            };
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
                        metaStoreParams, mpuBucket, keysToDelete,
                        aggregateETag, objMD,
                        extraPartLocations, pseudoCipherBundle, dataToDelete,
                        completeObjData);
                });
        },
        function storeAsNewObj(destinationBucket, dataLocations,
            metaStoreParams, mpuBucket, keysToDelete, aggregateETag,
             objMD, extraPartLocations, pseudoCipherBundle,
            dataToDelete, completeObjData, next) {
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
                    if (dataToDelete) {
                        data.batchDelete(dataToDelete, request.method, null,
                            logger.newRequestLoggerFromSerializedUids(log
                            .getSerializedUids()));
                    }
                    return next(null, mpuBucket, keysToDelete, aggregateETag,
                         extraPartLocations,
                        destinationBucket, generatedVersionId, completeObjData);
                });
        },
        function deletePartsMetadata(mpuBucket, keysToDelete, aggregateETag,
             extraPartLocations, destinationBucket,
            generatedVersionId, completeObjData, next) {
            services.batchDeleteObjectMetadata(mpuBucket.getName(),
                keysToDelete, log, err => next(err, destinationBucket,
                    aggregateETag, generatedVersionId));
            if (extraPartLocations && extraPartLocations.length > 0) {
                data.batchDelete(extraPartLocations, request.method, null,
                    logger.newRequestLoggerFromSerializedUids(log
                    .getSerializedUids()));
            }
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
                versionIdUtils.encode(generatedVersionId);
        }
        xmlParams.ETag = `"${aggregateETag}"`;
        const xml = _convertToXml(xmlParams);
        pushMetric('completeMultipartUpload', log, {
            authInfo,
            bucket: bucketName,
        });
        return callback(null, xml, resHeaders);
    });
}

module.exports = completeMultipartUpload;
