import async from 'async';
import xml from 'xml';

import config from '../../config';
import services from '../services';
import utils from '../utils';

const splitter = config.splitter;

  /*
  Format of xml response:
  <?xml version='1.0' encoding='UTF-8'?>
  <ListPartsResult xmlns='http://s3.amazonaws.com/doc/2006-03-01/'>
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
      <ETag>'7778aef83f66abc1fa1e8477f296d394'</ETag>
      <Size>10485760</Size>
    </Part>
    <Part>
      <PartNumber>3</PartNumber>
      <LastModified>2010-11-10T20:48:33.000Z</LastModified>
      <ETag>'aaaa18db4cc2f85cedef654fccc4a4x8'</ETag>
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
                xmlns: `http://${xmlParams.hostname}/doc/${dateString}`
            }
        },
        {Bucket: [xmlParams.bucketname]},
        {Key: [xmlParams.objectKey]},
        {UploadId: [xmlParams.uploadId]},
        {Initiator: [
            {ID: [xmlParams.initiator.id]},
            {DisplayName: [xmlParams.initiator.displayName]}
        ]},
    {Owner: [
        {ID: [xmlParams.owner.id]},
        {DisplayName: [xmlParams.owner.displayName]}
    ]},
        {StorageClass: [xmlParams.storageClass]}
    ];

    if (xmlParams.partNumberMarker !== 0) {
        listPartResultArray.push(
            {PartNumberMarker: [xmlParams.partNumberMarker]});
    }

    if (xmlParams.isTruncated) {
        listPartResultArray.push(
            {NextPartNumberMarker: [xmlParams.lastPartShown]});
    }
    listPartResultArray.push(
        {MaxParts: [xmlParams.maxParts]},
        {IsTruncated: [xmlParams.isTruncated]});
    for (let i = 0; i < xmlParams.partListing.length; i++) {
        listPartResultArray.push(
            {Part: [
                    {PartNumber: xmlParams.partListing[i].partNumber},
                    {LastModified: xmlParams.partListing[i].lastModified},
                    {ETag: xmlParams.partListing[i].etag},
                    {Size: xmlParams.partListing[i].size}
            ]});
    }

    const constructedJSON = {
        ListPartResult: listPartResultArray
    };
    return constructedJSON;
}

function _convertToXml(xmlParams) {
    const constructedJSON = _constructJSON(xmlParams);
    return xml(constructedJSON, { declaration: { encoding: 'UTF-8' }});
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
    let objectKey = utils.getResourceNames(request).object;
    const hostname = utils.getResourceNames(request).host;
    const bucketUID = utils.getResourceUID(request.namespace, bucketname);
    const uploadId = request.query.uploadId;
    const encoding = request.query['encoding-type'];
    const maxParts = Number.parseInt(request.query['max-parts'], 10) ?
        Number.parseInt(request.query['max-parts'], 10) : 1000;
    const partNumberMarker =
        Number.parseInt(request.query['part-number-marker'], 10) ?
        Number.parseInt(request.query['part-number-marker'], 10) : 0;
    const metadataValParams = {
        accessKey,
        bucketUID,
        objectKey,
        metastore,
        uploadId,
        requestType: 'listParts',
    };

    async.waterfall([
        function waterfall1(next) {
            services.checkBucketPolicies(metadataValParams, next);
        },
        function waterfall2(bucketPolicyGoAhead, next) {
            if (bucketPolicyGoAhead === 'accessGranted') {
                metadataValParams.requestType = 'bucketPolicyGoAhead';
            }
            services.metadataValidateMultipart(metadataValParams, next);
        },
        function waterfall3(mpuBucket, mpuOverviewArray, next) {
            const getPartsParams = {
                uploadId,
                mpuBucket,
                maxParts,
                partNumberMarker,
            };
            services.getSomeMPUparts(getPartsParams, (err, storedParts) => {
                if (err) {
                    return next(err);
                }
                return next(null, mpuBucket, storedParts, mpuOverviewArray);
            });
        }, function waterfall4(mpuBucket, storedParts, mpuOverviewArray, next) {
            if (encoding === 'url') {
                objectKey = encodeURIComponent(objectKey);
            }
            const initiator = {
                id: mpuOverviewArray[4],
                displayName: mpuOverviewArray[5]
            };
            const owner = {
                id: mpuOverviewArray[6],
                displayName: mpuOverviewArray[7]
            };
            const isTruncated = storedParts.IsTruncated;
            const partListing = storedParts.Contents.map((item) => {
                const fullKeyArray = item.key.split(splitter);
                return {
                    partNumber: fullKeyArray[1],
                    lastModified: fullKeyArray[2],
                    etag: fullKeyArray[3],
                    size: fullKeyArray[4],
                };
            });
            const lastPartShown = partListing.length > 0 ?
                partListing[partListing.length - 1].partNumber : null;
            const xmlParams = {
                bucketname,
                objectKey,
                hostname,
                uploadId,
                partNumberMarker,
                maxParts,
                encoding,
                initiator,
                owner,
                isTruncated,
                partListing,
                lastPartShown,
                storageClass: mpuOverviewArray[8],
            };
            const xml = _convertToXml(xmlParams);
            next(null, xml);
        }
    ], function waterfallFinal(err, xml) {
        return callback(err, xml);
    });
}
