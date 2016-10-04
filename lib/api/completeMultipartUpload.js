import { errors } from 'arsenal';
import async from 'async';
import xml from 'xml';
import crypto from 'crypto';
import { parseString } from 'xml2js';

import data from '../data/wrapper';
import constants from '../../constants';
import metadata from '../metadata/wrapper';
import services from '../services';

import { logger } from '../utilities/logger';

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

/*
  Construct JSON in proper format to be converted to XML
  to be returned to client
*/
function _constructJSON(xmlParams) {
    return {
        CompleteMultipartUploadResult: [
            { _attr: { xmlns: 'http://s3.amazonaws.com/doc/2006-03-01/' } },
            {
                Location: `http://${xmlParams.bucketName}`
                          + `.${xmlParams.hostname}/${xmlParams.objectKey}`,
            },
            { Bucket: [xmlParams.bucketName] },
            { Key: [xmlParams.objectKey] },
            { ETag: [xmlParams.ETag] },
        ],
    };
}

function _convertToXml(xmlParams) {
    const constructedJSON = _constructJSON(xmlParams);
    return xml(constructedJSON, { declaration: { encoding: 'UTF-8' } });
}

/**
 * completeMultipartUpload - Complete a multipart upload
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
export default
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

    function parseXml(xmlToParse, next) {
        return parseString(xmlToParse, (err, result) => {
            if (err || !result) {
                return next(errors.MalformedXML);
            }
            if (!result.CompleteMultipartUpload
                    || !result.CompleteMultipartUpload.Part) {
                return next(errors.MalformedPOSTRequest);
            }
            const jsonList = result.CompleteMultipartUpload;
            return next(null, jsonList);
        });
    }

    async.waterfall([
        function waterfall1(next) {
            services.metadataValidateMultipart(metadataValParams,
                (err, mpuBucket) => {
                    if (err) {
                        return next(err);
                    }
                    return next(null, mpuBucket);
                });
        },
        function waterfall2(mpuBucket, next) {
            if (request.post) {
                return parseXml(request.post, (err, jsonList) =>
                    next(err, mpuBucket, jsonList));
            }
            return next(errors.MalformedXML);
        },
        function waterfall3(mpuBucket, jsonList, next) {
            services.getMPUparts(mpuBucket.getName(), uploadId, log,
                (err, result) => {
                    const storedParts = result.Contents;
                    return next(err, mpuBucket, storedParts, jsonList);
                });
        },
        function waterfall4(mpuBucket, storedParts, jsonList, next) {
            const storedPartsAsObjects = storedParts.map(item => ({
                // In order to delete the part listing in the shadow
                // bucket, need the full key
                key: item.key,
                ETag: `"${item.value.ETag}"`,
                size: item.value.Size,

                locations: Array.isArray(item.value.partLocations) ?
                    item.value.partLocations : [item.value.partLocations],
            }));
            let splitter = constants.splitter;
            // BACKWARD: Remove to remove the old splitter
            if (mpuBucket.getMdBucketModelVersion() < 2) {
                splitter = constants.oldSplitter;
            }
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
            let calculatedSize = 0;
            // Check list sent to make sure valid
            // If the number of the parts in the JSON does not
            // match the number of parts stored, return error msg
            if (jsonList.Part.length !== storedPartsAsObjects.length) {
                return next(errors.InvalidPart);
            }
            for (let i = 0; i < jsonList.Part.length; i++) {
                const part = jsonList.Part[i];
                // If the complete list of parts sent with
                // the complete multipart upload request is not
                // in numerical order
                // return an error
                if (Number.parseInt(part.PartNumber[0], 10) !== i + 1) {
                    return next(errors.InvalidPartOrder);
                }

                // some clients send base64, convert to hex
                // 32 chars = 16 bytes(2 chars-per-byte) = 128 bits of MD5 hex
                let partETag = part.ETag[0].replace(/['"]/g, '');
                if (partETag.length !== 32) {
                    const buffered = new Buffer(part.ETag[0], 'base64')
                        .toString('hex');
                    partETag = `${buffered}`;
                }
                partETag = `"${partETag}"`;

                // If the list of parts sent with
                // the complete multipartupload request contains
                // a part ETag that does not match
                // the ETag for the part already sent, return an error
                if (partETag !== storedPartsAsObjects[i].ETag) {
                    return next(errors.InvalidPart);
                }
                // If any part other than the last part is less than 5MB,
                // return an error
                const partSize = Number.parseInt(storedPartsAsObjects[i]
                    .size, 10);
                if (i < jsonList.Part.length - 1 &&
                    partSize < constants.minimumAllowedPartSize) {
                    return next(errors.EntityTooSmall);
                }
                // Assemble array of part locations, aggregate size and build
                // string to create aggregate ETag

                // If part was put by a regular put part rather than a copy
                // it is always one location.  With a put part copy, could
                // be multiple locations so loop over array of locations.
                for (let j = 0; j < storedPartsAsObjects[i].locations.length;
                    j ++) {
                    // If the piece has parts (was a put part object
                    // copy) each piece will have a size attribute.
                    // Otherwise, the piece was put by a regular put
                    // part and the size the of the piece is the full
                    // part size.
                    const location = storedPartsAsObjects[i].locations[j];
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
                        cryptoScheme: location.sseCryptoScheme,
                        cipheredDataKey: location.sseCipheredDataKey,
                    };
                    dataLocations.push(pieceRetrievalInfo);
                    calculatedSize += pieceSize;
                }

                const partETagWithoutQuotes =
                    storedPartsAsObjects[i].ETag.slice(1, -1);
                concatETags += partETagWithoutQuotes;
            }
            // Convert the concatenated hex ETags to binary
            const bufferedHex = new Buffer(concatETags, 'hex');
            // Convert the buffer to a binary string
            const binaryString = bufferedHex.toString('binary');
            // Get the md5 of the binary string
            const md5Hash = crypto.createHash('md5');
            md5Hash.update(binaryString, 'binary');
            // Get the hex digest of the md5
            let aggregateETag = md5Hash.digest('hex');
            // Add the number of parts at the end
            aggregateETag = `${aggregateETag}-${jsonList.Part.length}`;

            // All is good so get the metadata stored when the mpu
            // was initiated and store everything as a new object

            // Reconstruct mpuOverviewKey to serve
            // as key to pull metadata originally stored when mpu initiated
            const mpuOverviewKey =
                `overview${splitter}${objectKey}${splitter}${uploadId}`;

            return metadata.getObjectMD(mpuBucket.getName(), mpuOverviewKey,
                log, (err, storedMetadata) => {
                    if (err) {
                        return next(err);
                    }
                    return next(null, mpuBucket, storedMetadata,
                        aggregateETag, calculatedSize,
                        dataLocations, mpuOverviewKey,
                        storedPartsAsObjects);
                });
        },
        function waterfall5(mpuBucket, storedMetadata, aggregateETag,
            calculatedSize, dataLocations, mpuOverviewKey,
            storedPartsAsObjects, next) {
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
                contentMD5: aggregateETag,
                size: calculatedSize,
                multipart: true,
                log,
            };
            const metadataValParams = {
                objectKey,
                authInfo,
                bucketName,
                // Required permissions for this action
                // at the destinationBucket level are same as objectPut
                requestType: 'objectPut',
                log,
            };
            services.metadataValidateAuthorization(metadataValParams,
                (err, destinationBucket, objMD) =>
                    next(err, destinationBucket, dataLocations, metaStoreParams,
                        mpuBucket, mpuOverviewKey, aggregateETag,
                        storedPartsAsObjects, objMD));
        },
        function waterfall6(destinationBucket, dataLocations,
                            metaStoreParams, mpuBucket, mpuOverviewKey,
                            aggregateETag, storedPartsAsObjects, objMD, next) {
            const serverSideEncryption =
                      destinationBucket.getServerSideEncryption();
            let pseudoCipherBundle = null;
            if (serverSideEncryption) {
                pseudoCipherBundle = {
                    algorithm: destinationBucket.getSseAlgorithm(),
                    masterKeyId: destinationBucket.getSseMasterKeyId(),
                };
            }
            next(null, destinationBucket, pseudoCipherBundle, dataLocations,
                 metaStoreParams, mpuBucket, mpuOverviewKey, aggregateETag,
                 storedPartsAsObjects, objMD);
        },
        function waterfall7(destinationBucket, pseudoCipherBundle,
                            dataLocations, metaStoreParams, mpuBucket,
                            mpuOverviewKey, aggregateETag,
                            storedPartsAsObjects, objMD, next) {
            services.metadataStoreObject(destinationBucket.getName(),
                dataLocations, pseudoCipherBundle, metaStoreParams, err => {
                    if (err) {
                        return next(err);
                    }
                    if (objMD && objMD.location) {
                        const dataToDelete = Array.isArray(objMD.location) ?
                            objMD.location : [objMD.location];
                        data.batchDelete(dataToDelete, logger
                            .newRequestLoggerFromSerializedUids(log
                            .getSerializedUids()));
                    }
                    return next(null, mpuBucket, mpuOverviewKey,
                        aggregateETag, storedPartsAsObjects);
                });
        },
        function waterfall8(mpuBucket, mpuOverviewKey, aggregateETag,
            storedPartsAsObjects, next) {
            const keysToDelete = storedPartsAsObjects.map(item => item.key);
            keysToDelete.push(mpuOverviewKey);
            services.batchDeleteObjectMetadata(mpuBucket.getName(),
                keysToDelete, log, err => next(err, aggregateETag));
        },
    ], (err, aggregateETag) => {
        xmlParams.ETag = `"${aggregateETag}"`;
        const xml = _convertToXml(xmlParams);
        return callback(err, xml);
    });
}
