import async from 'async';
import xml from 'xml';
import crypto from 'crypto';
import { parseString } from 'xml2js';

import Config from '../../lib/Config';
import data from '../data/wrapper';
import metadata from '../metadata/wrapper';
import services from '../services';
import utils from '../utils';

const splitter = new Config().splitter;

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

function _constructJSON(xmlParams) {
    const date = new Date();
    let month = (date.getMonth() + 1).toString();
    if (month.length === 1) {
        month = `0${month}`;
    }

    const dateString = `${date.getFullYear()}-${month}-${date.getDate()}`;

    return {
        CompleteMultipartUploadResult: [
            {
                _attr: {
                    xmlns: `http://${xmlParams.hostname}/doc/${dateString}`
                }
            },
            {
                Location: `http://${xmlParams.bucketName}`
                + `.${xmlParams.hostname}/${xmlParams.objectKey}`
            },
            { Bucket: [ xmlParams.bucketName ] },
            { Key: [ xmlParams.objectKey ] },
            { ETag: [ xmlParams.ETag ] },
        ]};
}

function _convertToXml(xmlParams) {
    const constructedJSON = _constructJSON(xmlParams);
    return xml(constructedJSON, { declaration: { encoding: 'UTF-8' }});
}

/**
 * completeMultipartUpload - Complete a multipart upload
 * @param  {string} accessKey - user's access key
 * @param  {object} metastore - in memory metadata store
 * @param  {object} request - http request object
 * @param  {function} log - Werelogs logger
 * @param {function} callback - callback to server
 */
export default function completeMultipartUpload(
    accessKey, metastore, request, log, callback) {
    const bucketName = utils.getResourceNames(request).bucket;
    const objectKey = utils.getResourceNames(request).object;
    const hostname = utils.getResourceNames(request).host;
    const uploadId = request.query.uploadId;
    const metadataValParams = {
        accessKey,
        bucketName,
        objectKey,
        metastore,
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
    async.waterfall([
        function waterfall1(next) {
            services.metadataValidateMultipart(metadataValParams,
                (err, mpuBucket, mpuOverviewArray)=> {
                    if (err) {
                        return next(err);
                    }
                    return next(null, mpuBucket, mpuOverviewArray);
                });
        },
        function waterfall2(mpuBucket, mpuOverviewArray, next) {
            let xmlToParse = request.post;
            // For AWS CLI, the router is parsing the xml as an object
            // so have to reconstruct an xml string.
            if (typeof xmlToParse === 'object') {
                xmlToParse = '<CompleteMultipartUpload xmlns='
                    .concat(xmlToParse['<CompleteMultipartUpload xmlns']);
            }
            return parseString(xmlToParse, function parseXML(err, result) {
                if (err) {
                    return next('MalformedXML');
                }
                if (!result.CompleteMultipartUpload
                        || !result.CompleteMultipartUpload.Part) {
                    return next('MalformedPOSTRequest');
                }
                const jsonList = result.CompleteMultipartUpload;
                return next(null, mpuBucket, jsonList, mpuOverviewArray);
            });
        },
        function waterfall3(mpuBucket, jsonList, mpuOverviewArray, next) {
            services.getMPUparts(mpuBucket.name, uploadId,
                (err, storedParts) => {
                    return next(err, mpuBucket, storedParts,
                        jsonList, mpuOverviewArray);
                });
        },
        function waterfall4(mpuBucket, storedParts,
            jsonList, mpuOverviewArray, next) {
            const sortedStoredParts = storedParts.sort((a, b) => {
                const aArray = a.key.split(splitter);
                const bArray = b.key.split(splitter);
                const aPartNumber = Number.parseInt(aArray[1], 10);
                const bPartNumber = Number.parseInt(bArray[1], 10);
                    // If duplicates, sort so that last modified comes second
                if (aPartNumber - bPartNumber === 0) {
                    const aTime = Date.parse(aArray[2]);
                    const bTime = Date.parse(bArray[2]);
                    return aTime - bTime;
                }
                // Return numbers in ascending order
                return aPartNumber - bPartNumber;
            });
            // Remove any duplicates.  The first of any duplicate should
            // be removed because due to sort above that will be the earlier
            // part. Remove duplicate from sortedStoredParts array and save
            // the unnecessary parts to the duplicatePartsToDelete array
            let duplicatePartsToDelete = [];
            for (let i = 0; i < sortedStoredParts.length - 1; i++) {
                const currentPartNumber =
                    sortedStoredParts[i].key.split(splitter)[1];
                const nextPartNumber =
                    sortedStoredParts[i + 1].key.split(splitter)[1];
                if (currentPartNumber === nextPartNumber) {
                    duplicatePartsToDelete.push(sortedStoredParts[i]);
                    sortedStoredParts.splice(i, 1);
                }
            }

            // Modfy the duplicatePartsToDelete array so it only contains
            // the locations in data for each of the parts to be deleted.
            duplicatePartsToDelete = duplicatePartsToDelete.map((item) => {
                return item.key.split(splitter)[5].split(',');
            });
            const storedPartsAsObjects = sortedStoredParts.map((item) => {
                const arrayItem = item.key.split(splitter);
                return {
                    key: item.key,
                    // Conforming to HTTP standard ETag's hex should always
                    // be enclosed in quotes
                    ETag: `"${arrayItem[3]}"`,
                    size: arrayItem[4],
                    location: arrayItem[5].split(','),
                };
            });
            let dataLocations = [];
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
            for (let i = 0; i < jsonList.Part.length; i++) {
                const part = jsonList.Part[i];
                // If the complete list of parts sent with
                // the complete multipart upload request is not
                // in numerical order
                // return an error
                if (Number.parseInt(part.PartNumber[0], 10) !== i + 1) {
                    return next('InvalidPartOrder');
                }

                // some clients send base64, convert to hex
                // 32 chars = 16 bytes(2 chars-per-byte) = 128 bits of MD5 hex
                const partETag = part.ETag[0].replace(/"/g, '');
                if (partETag.length !== 32) {
                    const buffered = new Buffer(part.ETag[0], 'base64')
                        .toString('hex');
                    part.ETag[0] = `"${buffered}"`;
                }

                // If the list of parts sent with
                // the complete multipartupload request contains
                // a part ETag that does not match
                // the ETag for the part already sent, return an error
                if (part.ETag[0] !== storedPartsAsObjects[i].ETag) {
                    return next('InvalidPart');
                }
                // If any part other than the last part is less than 5MB,
                // return an error
                if (i < jsonList.Part.length - 1 &&
                    Number.parseInt(storedPartsAsObjects[i]
                        .size, 10) < 5242880) {
                    return next('EntityTooSmall');
                }
                // Assemble array of part locations, aggregate size and build
                // string to create aggregate ETag
                dataLocations.push(storedPartsAsObjects[i].location);
                calculatedSize +=
                    Number.parseInt(storedPartsAsObjects[i].size, 10);
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
            md5Hash.update(binaryString);
            // Get the hex digest of the md5
            let aggregateETag = md5Hash.digest('hex');
            // Add the number of parts at the end
            aggregateETag = `${aggregateETag}-${jsonList.Part.length}`;

            // All is good so get the metadata stored when the mpu
            // was initiated and store everything as a new object

            // Convert mpuOverviewArray back into a string to serve
            // as key to pull metadata originally stored when mpu initiated
            const mpuOverviewKey = mpuOverviewArray.join(splitter);

            dataLocations = dataLocations.reduce((a, b) => a.concat(b));
            metadata.getObjectMD(mpuBucket.name, mpuOverviewKey,
                (err, storedMetadata) => {
                    if (err) {
                        return next(err);
                    }
                    return next(null, mpuBucket, storedMetadata,
                        aggregateETag, calculatedSize,
                        dataLocations, mpuOverviewKey,
                        storedPartsAsObjects, duplicatePartsToDelete);
                });
        },
        function waterfall5(mpuBucket, storedMetadata, aggregateETag,
            calculatedSize, dataLocations, mpuOverviewKey,
            storedPartsAsObjects, duplicatePartsToDelete, next) {
            const metaHeaders = {};
            const keysNotNeeded =
                ['owner', 'initiator', 'partLocations', 'key',
                'initiated', 'uploadId', 'content-type', 'expires'];
            const metadataKeysToPull =
                Object.keys(storedMetadata).filter((item) => {
                    if (keysNotNeeded.indexOf(item) === -1) {
                        return item;
                    }
                });
            metadataKeysToPull.forEach((item) => {
                metaHeaders[item] = storedMetadata[item];
            });

            const metaStoreParams = {
                accessKey,
                objectKey,
                metaHeaders,
                uploadId,
                metastore,
                contentType: storedMetadata['content-type'],
                contentMD5: aggregateETag,
                size: calculatedSize,
                multipart: true,
            };
            metadata.getBucket(bucketName, (err, destinationBucket) => {
                return next(err, destinationBucket, dataLocations,
                            metaStoreParams, mpuBucket, mpuOverviewKey,
                            aggregateETag, storedPartsAsObjects,
                            duplicatePartsToDelete);
            });
        },
        function waterfall6(destinationBucket, dataLocations,
            metaStoreParams, mpuBucket, mpuOverviewKey, aggregateETag,
            storedPartsAsObjects, duplicatePartsToDelete, next) {
            services.metadataStoreObject(destinationBucket.name, undefined,
                dataLocations, metaStoreParams, (err) => {
                    if (err) {
                        return next(err);
                    }
                    return next(null, mpuBucket, mpuOverviewKey,
                        aggregateETag, storedPartsAsObjects,
                        duplicatePartsToDelete);
                });
        },
        function waterfall7(mpuBucket, mpuOverviewKey, aggregateETag,
            storedPartsAsObjects, duplicatePartsToDelete, next) {
            const keysToDelete = storedPartsAsObjects.map(item => item.key);
            keysToDelete.push(mpuOverviewKey);
            services.batchDeleteObjectMetadata(mpuBucket.name,
                keysToDelete, (err) => {
                    return next(err, duplicatePartsToDelete, aggregateETag);
                });
        },
        function waterfall8(duplicatePartsToDelete, aggregateETag, next) {
            if (duplicatePartsToDelete.length === 0) {
                return next(null, aggregateETag);
            }
            data.delete(duplicatePartsToDelete, (err) => {
                if (err) {
                    return next(err);
                }
                return next(null, aggregateETag);
            });
        }
    ], function finalfunc(err, aggregateETag) {
        xmlParams.ETag = aggregateETag;
        const xml = _convertToXml(xmlParams);
        return callback(err, xml);
    });
}
