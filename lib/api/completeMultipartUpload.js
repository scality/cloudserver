import { errors } from 'arsenal';
import async from 'async';
import crypto from 'crypto';
import { parseString } from 'xml2js';
import escapeForXML from '../utilities/escapeForXML';
import { pushMetric } from '../utapi/utilities';

import data from '../data/wrapper';
import collectCorsHeaders from '../utilities/collectCorsHeaders';
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
                `${escapeForXML(xmlParams.hostname)}/` +
                `${escapeForXML(xmlParams.objectKey)}</Location>`,
             `<Bucket>${xmlParams.bucketName}</Bucket>`,
             `<Key>${escapeForXML(xmlParams.objectKey)}</Key>`,
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
            if (err || !result || !result.CompleteMultipartUpload
                || !result.CompleteMultipartUpload.Part) {
                return next(errors.MalformedXML);
            }
            const jsonList = result.CompleteMultipartUpload;
            return next(null, jsonList);
        });
    }

    async.waterfall([
        function waterfall1(next) {
            const metadataValParams = {
                objectKey,
                authInfo,
                bucketName,
                // Required permissions for this action
                // at the destinationBucket level are same as objectPut
                requestType: 'objectPut',
                log,
            };
            services.metadataValidateAuthorization(metadataValParams, next);
        },
        function waterfall2(destBucket, objMD, next) {
            services.metadataValidateMultipart(metadataValParams,
                (err, mpuBucket) => {
                    if (err) {
                        return next(err, destBucket);
                    }
                    return next(null, destBucket, objMD, mpuBucket);
                });
        },
        function waterfall3(destBucket, objMD, mpuBucket, next) {
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
        function waterfall4(destBucket, objMD, mpuBucket, jsonList, next) {
            services.getMPUparts(mpuBucket.getName(), uploadId, log,
                (err, result) => {
                    if (err) {
                        return next(err, destBucket);
                    }
                    const storedParts = result.Contents;
                    return next(null, destBucket, objMD, mpuBucket,
                        storedParts, jsonList);
                });
        },
        function waterfall5(destBucket, objMD, mpuBucket, storedParts, jsonList,
        next) {
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
            const partLength = jsonList.Part.length;

            console.log('storedPartsAsObjects', JSON.stringify(storedPartsAsObjects, null, 4))

            // A user can put more parts than they end up including
            // in the completed MPU but there cannot be more
            // parts in the complete message than were already put
            if (partLength > storedPartsAsObjects.length) {
                console.log('===========================')
                console.log('Cannot have more parts in complete message than were put')
                console.log(`partLength (${partLength} > storedPartsAsObjects.length (${storedPartsAsObjects.length})`)
                console.log('===========================')
                return next(errors.InvalidPart, destBucket);
            }
            // if extra parts, need to delete the data when done with completing
            // mpu so extract the info to delete here
            const extraPartLocations = [];
            if (partLength < storedPartsAsObjects.length) {
                const extraParts = storedPartsAsObjects.slice(partLength);
                extraParts.forEach(part => {
                    const locations = part.locations;
                    locations.forEach(location => {
                        if (!location || typeof location !== 'object') {
                            return;
                        }
                        extraPartLocations.push({ key: location.key });
                    });
                });
            }

            for (let i = 0; i < partLength; i++) {
                const part = jsonList.Part[i];
                // If the complete list of parts sent with
                // the complete multipart upload request is not
                // in numerical order
                // return an error
                const partNumber = Number.parseInt(part.PartNumber[0], 10);
                if (partNumber !== i + 1) {
                    // If not in order return InvalidPartOrder
                    console.log('===========================')
                    console.log(`i = ${i}`)
                    console.log('Parts listed are not in numerical order')
                    console.log(`partNumber (${partNumber}) !== i + 1 (${i + 1})`)
                    console.log('===========================')
                    if (partNumber <= partLength) {
                        return next(errors.InvalidPartOrder, destBucket);
                    }
                    console.log('missing final part')
                    // If missing part return InvalidPart
                    return next(errors.InvalidPart, destBucket);
                }

                // some clients send base64, convert to hex
                // 32 chars = 16 bytes(2 chars-per-byte) = 128 bits of MD5 hex
                let partETag = part.ETag[0].replace(/['"]/g, '');
                if (partETag.length !== 32) {
                    const buffered = Buffer.from(part.ETag[0], 'base64')
                        .toString('hex');
                    partETag = `${buffered}`;
                }
                partETag = `"${partETag}"`;
                // If the list of parts sent with
                // the complete multipartupload request contains
                // a part ETag that does not match
                // the ETag for the part already sent, return an error
                if (partETag !== storedPartsAsObjects[i].ETag) {
                    console.log('===========================')
                    console.log(`i = ${i}`)
                    console.log('ETag for a part does not match one uploaded')
                    console.log(`partNumber ${partNumber} ETag ${partETag} !== storedPartsAsObjects[${i}].ETag ${storedPartsAsObjects[i].ETag}`)
                    console.log('===========================')
                    return next(errors.InvalidPart, destBucket);
                }
                // If any part other than the last part is less than 5MB,
                // return an error
                const partSize = Number.parseInt(storedPartsAsObjects[i]
                    .size, 10);

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
            console.log('===========================')
            console.log(`aggregateETag: ${aggregateETag}`);
            console.log('===========================')

            // All is good so get the metadata stored when the mpu
            // was initiated and store everything as a new object

            // Reconstruct mpuOverviewKey to serve
            // as key to pull metadata originally stored when mpu initiated
            const mpuOverviewKey =
                `overview${splitter}${objectKey}${splitter}${uploadId}`;

            console.log('===========================')
            console.log('About fetch object MD for mpuOverViewKey.')
            console.log('===========================')
            return metadata.getObjectMD(mpuBucket.getName(), mpuOverviewKey,
                log, (err, storedMetadata) => {
                    if (err) {
                        console.log('===========================')
                        console.log('Encountered error fetching object MD.');
                        console.log('Err', err);
                        console.log('===========================')
                        return next(err, destBucket);
                    }
                    console.log('===========================')
                    console.log('Successfully fetched objectMD.');
                    console.log('mpuOverView objectMD:', storedMetadata);
                    console.log('About to enter waterfall 6.');
                    console.log('===========================')
                    return next(null, destBucket, objMD, mpuBucket,
                        storedMetadata, aggregateETag, calculatedSize,
                        dataLocations, mpuOverviewKey,
                        storedPartsAsObjects, extraPartLocations);
                });
        },
        function waterfall6(destBucket, objMD, mpuBucket, storedMetadata,
            aggregateETag, calculatedSize, dataLocations, mpuOverviewKey,
            storedPartsAsObjects, extraPartLocations, next) {
            console.log('===========================')
            console.log('Entered waterfall 6.');
            console.log('Assembling metadataKeysToPull and metaStoreParams');
            console.log('===========================')
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
                log,
            };
            console.log('===========================')
            console.log('About to enter waterfall7.');
            console.log('===========================')
            next(null, destBucket, dataLocations, metaStoreParams,
                mpuBucket, mpuOverviewKey, aggregateETag,
                storedPartsAsObjects, objMD, extraPartLocations);
        },
        function waterfall7(destinationBucket, dataLocations,
                            metaStoreParams, mpuBucket, mpuOverviewKey,
                            aggregateETag, storedPartsAsObjects, objMD,
                            extraPartLocations, next) {
            console.log('===========================')
            console.log('Entered waterfall7.');
            console.log('About to store completed mpu metadata in metadata');
            console.log('===========================')
            const serverSideEncryption =
                      destinationBucket.getServerSideEncryption();
            let pseudoCipherBundle = null;
            if (serverSideEncryption) {
                pseudoCipherBundle = {
                    algorithm: destinationBucket.getSseAlgorithm(),
                    masterKeyId: destinationBucket.getSseMasterKeyId(),
                };
            }
            services.metadataStoreObject(destinationBucket.getName(),
                dataLocations, pseudoCipherBundle, metaStoreParams, err => {
                    if (err) {
                        console.log('===========================')
                        console.log('Encountered error storing metadata.');
                        console.log('Err', err);
                        console.log('===========================')
                        return next(err, destinationBucket);
                    }
                    console.log('===========================')
                    console.log('Successfully stored metadata');
                    console.log('About to batchdelete part data');
                    console.log('===========================')
                    if (objMD && objMD.location) {
                        console.log('Detected need to batchdelete (objMD & objMD.location exist)')
                        const dataToDelete = Array.isArray(objMD.location) ?
                            objMD.location : [objMD.location];
                        data.batchDelete(dataToDelete, logger
                            .newRequestLoggerFromSerializedUids(log
                            .getSerializedUids()));
                    }
                    console.log('===========================')
                    console.log('Sent batchdelete request if detected need.');
                    console.log('About to enter waterfall8', err);
                    console.log('===========================')
                    return next(null, mpuBucket, mpuOverviewKey,
                        aggregateETag, storedPartsAsObjects,
                        extraPartLocations, destinationBucket);
                });
        },
        function waterfall8(mpuBucket, mpuOverviewKey, aggregateETag,
            storedPartsAsObjects, extraPartLocations, destinationBucket, next) {
            console.log('===========================')
            console.log('Entered waterfall8');
            console.log('About to assemble objectMD of parts to delete');
            console.log('===========================')
            const keysToDelete = storedPartsAsObjects.map(item => item.key);
            keysToDelete.push(mpuOverviewKey);
            console.log('===========================')
            console.log('keysToDelete length', keysToDelete.length);
            console.log('About to batchdelete object MD and try to enter waterfall callback');
            console.log('===========================')
            services.batchDeleteObjectMetadata(mpuBucket.getName(),
                keysToDelete, log, err => next(err, destinationBucket,
                    aggregateETag));
            console.log('===========================')
            console.log('Sent batchdelete objectMD request w/ callback to waterfall callback, also checking for extra parts to delete');
            console.log('===========================')
            if (extraPartLocations.length > 0) {
                console.log('===========================')
                console.log('Detected extra parts to delete, sending batch delete request');
                console.log('===========================')
                data.batchDelete(extraPartLocations, logger
                    .newRequestLoggerFromSerializedUids(log
                    .getSerializedUids()));
            }
        },
    ], (err, destinationBucket, aggregateETag) => {
        console.log('===========================')
        console.log('Entered waterfall callback with these parameters:');
        console.log('err', err);
        console.log('destinationBucket', destinationBucket);
        console.log('aggregateETag', aggregateETag);
        console.log('===========================')
        const corsHeaders =
            collectCorsHeaders(request.headers.origin, request.method,
                destinationBucket);
        if (err) {
            console.log('===========================')
            console.log('Attempting to return to routePOST callback with err');
            console.log('===========================')
            return callback(err, null, corsHeaders);
        }
        xmlParams.ETag = `"${aggregateETag}"`;
        const xml = _convertToXml(xmlParams);
        pushMetric('completeMultipartUpload', log, {
            authInfo,
            bucket: bucketName,
        });
        console.log('===========================')
        console.log('Attempting to return to routePOST callback without err');
        console.log('===========================')
        return callback(null, xml, corsHeaders);
    });
}
