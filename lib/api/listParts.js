import xml from 'xml';

import utils from '../utils';
import services from '../services';


  /*
  Format of xml response:
  <?xml version="1.0" encoding="UTF-8"?>
  <ListPartsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Bucket>example-bucket</Bucket>
    <Key>example-object</Key>
    <UploadId>XXBsb2FkIElEIGZvciBlbHZpbmcncyVcdS1
    tb3ZpZS5tMnRzEEEwbG9hZA</UploadId>
    <Initiator>
        <ID>arn:aws:iam::111122223333:user/some-user-11116a31-
        17b5-4fb7-9df5-b288870f11xx</ID>
        <DisplayName>umat-user-11116a31-17b5-4fb7-9df5-
        b288870f11xx</DisplayName>
    </Initiator>
    <Owner>
      <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
      <DisplayName>someName</DisplayName>
    </Owner>
    <StorageClass>STANDARD</StorageClass>
    <PartNumberMarker>1</PartNumberMarker>
    <NextPartNumberMarker>3</NextPartNumberMarker>
    <MaxParts>2</MaxParts>
    <IsTruncated>true</IsTruncated>
    <Part>
      <PartNumber>2</PartNumber>
      <LastModified>2010-11-10T20:48:34.000Z</LastModified>
      <ETag>"7778aef83f66abc1fa1e8477f296d394"</ETag>
      <Size>10485760</Size>
    </Part>
    <Part>
      <PartNumber>3</PartNumber>
      <LastModified>2010-11-10T20:48:33.000Z</LastModified>
      <ETag>"aaaa18db4cc2f85cedef654fccc4a4x8"</ETag>
      <Size>10485760</Size>
    </Part>
  </ListPartsResult>
   */

function _constructJSON(xmlParams) {
    const date = new Date();
    let month = (date.getMonth() + 1).toString();
    if (month.length === 1) {
        month = `0${month}`;
    }

    const dateString = `${date.getFullYear()}-${month}-${date.getDate()}`;
    const listPartResultArray = [
        {
            _attr: {
                "xmlns": `http:\/\/${xmlParams.hostname}/doc/${dateString}`
            }
        },
        {"Bucket": [xmlParams.bucketname]},
        {"Key": [xmlParams.objectKey]},
        {"UploadId": [xmlParams.uploadId]},
        {"Initiator": [
            {"ID": [xmlParams.initiator.id]},
            {"DisplayName": [xmlParams.initiator.displayName]}
        ]},
    {"Owner": [
        {"ID": [xmlParams.owner.id]},
        {"DisplayName": [xmlParams.owner.displayName]}
    ]},
        {"StorageClass": [xmlParams.storageClass]}
    ];

    if (xmlParams.partNumberMarker !== 0) {
        listPartResultArray.push(
            {"PartNumberMarker": [xmlParams.partNumberMarker]});
    }

    // The first part included should be the part following the partNumberMarker
    const placeToStart = xmlParams.partNumberMarker + 1;
    // Include parts up to the lesser of:
    // the length of the stored parts or
    // the specified max parts plus the starting place account for an offset
    const itemsToInclude =
        Math.min(xmlParams.partListing.length,
            xmlParams.maxParts + placeToStart);
    const lastPartShown = itemsToInclude - 1;
    if (xmlParams.isTruncated) {
        listPartResultArray.push(
            {"NextPartNumberMarker": [lastPartShown]});
    }
    listPartResultArray.push(
        {"MaxParts": [xmlParams.maxParts]},
        {"IsTruncated": [xmlParams.isTruncated]});
    for (let i = placeToStart; i < itemsToInclude; i++) {
        listPartResultArray.push(
            {"Part": [
                    {"PartNumber": i},
                    {"LastModified": xmlParams.partListing[i].lastModified},
                    {"ETag": xmlParams.partListing[i].etag},
                    {"Size": xmlParams.partListing[i].size}
            ]});
    }

    const constructedJSON = {
        "ListPartResult": listPartResultArray
    };
    return constructedJSON;
}

function _convertToXml(xmlParams, encoding = 'UTF-8') {
    const constructedJSON = _constructJSON(xmlParams);
    return xml(constructedJSON, { declaration: { encoding: encoding }});
}

/**
 * listParts - List parts of an open multipart upload
 * @param  {string} accessKey - user's access key
 * @param  {object} metastore - in memory metadata store
 * @param  {object} request - http request object
 * @param {function} callback - callback to server
 * @return {function} calls callback to router with error
 * or xml as arguments
 */
export default function listParts(
    accessKey, metastore, request, callback) {
    const bucketname = utils.getResourceNames(request).bucket;
    const objectKey = utils.getResourceNames(request).object;
    const hostname = utils.getResourceNames(request).host;
    const bucketUID = utils.getResourceUID(request.namespace, bucketname);
    const uploadId = request.query.uploadId;
    const encoding = request.lowerCaseHeaders['encoding-type'];
    const maxParts =
        Number.parseInt(request.lowerCaseHeaders['max-parts'], 10) || 1000;
    const partNumberMarker =
        Number.parseInt(request
            .lowerCaseHeaders['part-number-marker'], 10) || 0;
    const metadataValParams = {
        accessKey,
        bucketUID,
        objectKey,
        metastore,
        uploadId,
        requestType: 'listParts',
    };

    services.metadataValidateMultipart(metadataValParams,
        function parseMD(err, bucket, multipartMetadata) {
            if (err) {
                return callback(err);
            }
            const partListing = multipartMetadata.partLocations;
            // Note that the 0 index of partListing is null so have
            // to subtract 1 in defining isTruncated
            const isTruncated =
                (partListing.length - partNumberMarker - 1) > maxParts ?
                    true : false;
            const xmlParams = {
                bucketname,
                objectKey,
                hostname,
                uploadId,
                partNumberMarker,
                maxParts,
                isTruncated,
                partListing,
                initiator: multipartMetadata.initiator,
                owner: multipartMetadata.owner,
                storageClass: multipartMetadata['x-amz-storage-class'],
            };
            const xml = _convertToXml(xmlParams, encoding);
            return callback(null, xml);
        });
}
