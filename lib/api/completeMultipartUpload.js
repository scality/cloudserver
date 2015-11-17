import { parseString } from 'xml2js';
import utils from '../utils';
import services from '../services';
import async from 'async';
import xml from 'xml';

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
      <?xml version="1.0" encoding="UTF-8"?>
    <CompleteMultipartUploadResult
    xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
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
        "CompleteMultipartUploadResult": [
            {
                _attr: {
                    "xmlns": `http:\/\/${xmlParams.hostname}/doc/${dateString}`
                }
            },
            {
                "Location": `http:\/\/${xmlParams.bucketname}`
                + `.${xmlParams.hostname}/${xmlParams.objectKey}`
            },
            {
                "Bucket": [xmlParams.bucketname]
            },
            {
                "Key": [xmlParams.objectKey]
            },
            {
                "ETag": [xmlParams.etag]
            }
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
 * @param {function} callback - callback to server
 */
export default function completeMultipartUpload(
    accessKey, metastore, request, callback) {
    const bucketname = utils.getResourceNames(request).bucket;
    const objectKey = utils.getResourceNames(request).object;
    const hostname = utils.getResourceNames(request).host;
    const bucketUID = utils.getResourceUID(request.namespace, bucketname);
    const objectUID =
        utils.getResourceUID(request.namespace, bucketname + objectKey);
    const uploadId = request.query.uploadId;
    // TODO: Decide what to return for the combined etag.  AWS says it
    // might or might not return the MD5 of the complete object
    const fakeEtag = '3858f62230ac3c915f300c664312c11f-9';
    const metadataValParams = {
        accessKey,
        bucketUID,
        objectKey,
        metastore,
        uploadId,
    };
    const xmlParams = {
        bucketname,
        objectKey,
        hostname,
        etag: fakeEtag,
    };

    async.waterfall([
        function waterfall1(next) {
            services.metadataValidateMultipart(metadataValParams, next);
        },
        function waterfall2(bucket, multipartMetadata, next) {
            const xmlToParse = request.post;
            return parseString(xmlToParse,
                function parseXML(err, result) {
                    if (err) {
                        return next('MalformedXML');
                    }
                    if (!result.CompleteMultipartUpload
                            || !result.CompleteMultipartUpload.Part) {
                        return next('MalformedPOSTRequest');
                    }
                    const jsonList = result
                        .CompleteMultipartUpload;
                    return next(null, bucket, multipartMetadata, jsonList);
                });
        },
        function waterfall3(bucket, multipartMetadata, jsonList, next) {
            const dataLocations = [];
            let calculatedSize = 0;
            // Check list sent to make sure valid
            for (let i = 0; i < jsonList.Part.length; i ++) {
                let actualPartNumber;
                let part;
                // The array storing the actual parts has a null value in the
                // first (0th) position so need to add 1 to the incrementor
                // so that the jsonList which does start at 0 matches up with
                // thhe stored metadata which starts at 1.
                actualPartNumber = i + 1;
                part = jsonList.Part[i];
                // If the complete list of parts sent with
                // the complete multipart upload request are not
                // in numerical order
                // return an error
                if (Number.parseInt(part.PartNumber[0], 10)
                    !== actualPartNumber) {
                    return next('InvalidPartOrder');
                }
                // If the list of parts sent with
                // the complete multipartupload request contains
                // a part etag that does not match
                // the etag for the part already sent, return an error
                if (part.ETag[0] !==
                    multipartMetadata.partLocations[actualPartNumber].etag) {
                    return next('InvalidPart');
                }
                // If any part other than the last part is less than 5MB,
                // return an error
                if (actualPartNumber < jsonList.Part.length &&
                    Number.parseInt(multipartMetadata
                        .partLocations[actualPartNumber].size, 10) < 5242880) {
                    return next('EntityTooSmall');
                }
                // Assemble array of part locations and aggregate size
                if (actualPartNumber <= jsonList.Part.length) {
                    dataLocations.push(
                        multipartMetadata
                        .partLocations[actualPartNumber].location);
                    calculatedSize += Number.parseInt(multipartMetadata
                        .partLocations[actualPartNumber].size, 10);
                }
            }

            const metaHeaders = {};
            const keysNotNeeded =
                ['owner', 'initiator', 'partLocations', 'key',
                'initiated', 'uploadId', 'content-type', 'expires'];
            const metadataKeysToPull =
                Object.keys(multipartMetadata).filter((item) => {
                    if (keysNotNeeded.indexOf(item) === -1) {
                        return item;
                    }
                });
            metadataKeysToPull.forEach((item) => {
                metaHeaders[item] = multipartMetadata[item];
            });

            // All is good so store the combined
            // object as a new object in metadata
            const metaStoreParams = {
                accessKey,
                objectKey,
                objectUID,
                metaHeaders,
                uploadId,
                contentType: multipartMetadata['content-type'],
                contentMD5: fakeEtag,
                size: calculatedSize,
                multipart: true,
            };
            services.metadataStoreObject(bucket, undefined,
                dataLocations, metaStoreParams, next);
        }
    ], function finalfunc(err) {
        const xml = _convertToXml(xmlParams);
        return callback(err, xml);
    });
}
