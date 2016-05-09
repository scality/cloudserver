import async from 'async';
import xml from 'xml';
import querystring from 'querystring';

import constants from '../../constants';
import services from '../services';

const splitter = constants.splitter;

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


/*
  Construct JSON in proper format to be converted to XML
  to be returned to client
*/
function _constructJSON(xmlParams) {
    const listPartResultArray = [
        { _attr: { xmlns: 'http://s3.amazonaws.com/doc/2006-03-01/' } },
        { Bucket: [xmlParams.bucketName] },
        { Key: [xmlParams.objectKey] },
        { UploadId: [xmlParams.uploadId] },
        { Initiator: [
            { ID: [xmlParams.initiator.id] },
            { DisplayName: [xmlParams.initiator.displayName] },
        ] },
        { Owner: [
            { ID: [xmlParams.owner.id] },
            { DisplayName: [xmlParams.owner.displayName] },
        ] },
        { StorageClass: [xmlParams.storageClass] },
    ];

    if (xmlParams.partNumberMarker !== 0) {
        listPartResultArray.push(
            { PartNumberMarker: [xmlParams.partNumberMarker] });
    }

    if (xmlParams.isTruncated) {
        listPartResultArray.push(
            { NextPartNumberMarker: [xmlParams.lastPartShown] });
    }
    listPartResultArray.push(
        { MaxParts: [xmlParams.maxParts] },
        { IsTruncated: [xmlParams.isTruncated] });
    for (let i = 0; i < xmlParams.partListing.length; i++) {
        listPartResultArray.push({
            Part: [
                { PartNumber: Number.parseInt(xmlParams
                    .partListing[i].partNumber, 10) },
                { LastModified: xmlParams.partListing[i].lastModified },
                { ETag: xmlParams.partListing[i].ETag },
                { Size: xmlParams.partListing[i].size },
            ],
        });
    }

    const constructedJSON = { ListPartResult: listPartResultArray };
    return constructedJSON;
}

function _convertToXml(xmlParams) {
    const constructedJSON = _constructJSON(xmlParams);
    return xml(constructedJSON, { declaration: { encoding: 'UTF-8' } });
}

/**
 * listParts - List parts of an open multipart upload
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
export default function listParts(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'listParts' });

    const bucketName = request.bucketName;
    let objectKey = request.objectKey;
    const uploadId = request.query.uploadId;
    const encoding = request.query['encoding-type'];
    const maxParts = Number.parseInt(request.query['max-parts'], 10) ?
        Number.parseInt(request.query['max-parts'], 10) : 1000;
    const partNumberMarker =
        Number.parseInt(request.query['part-number-marker'], 10) ?
        Number.parseInt(request.query['part-number-marker'], 10) : 0;
    const metadataValMPUparams = {
        authInfo,
        bucketName,
        objectKey,
        uploadId,
        requestType: 'listParts',
        log,
    };
    // For validating the request at the destinationBucket level
    // params are the same as validating at the MPU level
    // but the requestType is the more general 'objectPut'
    // (on the theory that if you are listing the parts of
    // an MPU being put you should have the right to put
    // the object as a prerequisite)
    const metadataValParams = Object.assign({}, metadataValMPUparams);
    metadataValParams.requestType = 'objectPut';

    async.waterfall([
        function checkDestBucketVal(next) {
            services.metadataValidateAuthorization(metadataValParams,
                (err, destinationBucket) => {
                    if (err) {
                        return next(err);
                    }
                    if (destinationBucket.policies) {
                        // TODO: Check bucket policies to see if user is granted
                        // permission or forbidden permission to take
                        // given action.
                        // If permitted, add 'bucketPolicyGoAhead'
                        // attribute to params for validating at MPU level.
                        // This is GH Issue#76
                        metadataValMPUparams.requestType =
                            'bucketPolicyGoAhead';
                    }
                    return next();
                });
        },
        function waterfall2(next) {
            services.metadataValidateMultipart(metadataValMPUparams, next);
        },
        function waterfall3(mpuBucket, mpuOverviewArray, next) {
            const getPartsParams = {
                uploadId,
                mpuBucketName: mpuBucket.name,
                maxParts,
                partNumberMarker,
                log,
            };
            services.getSomeMPUparts(getPartsParams, (err, storedParts) => {
                if (err) {
                    return next(err);
                }
                return next(null, mpuBucket, storedParts, mpuOverviewArray);
            });
        }, function waterfall4(mpuBucket, storedParts, mpuOverviewArray, next) {
            if (encoding === 'url') {
                objectKey = querystring.escape(objectKey);
            }
            const initiator = {
                id: mpuOverviewArray[4],
                displayName: mpuOverviewArray[5],
            };
            const owner = {
                id: mpuOverviewArray[6],
                displayName: mpuOverviewArray[7],
            };
            const isTruncated = storedParts.IsTruncated;
            const partListing = storedParts.Contents.map((item) => {
                const fullKeyArray = item.key.split(splitter);
                return {
                    partNumber: fullKeyArray[1],
                    lastModified: item.value.LastModified,
                    ETag: item.value.ETag,
                    size: item.value.Size,
                };
            });
            const lastPartShown = partListing.length > 0 ?
                partListing[partListing.length - 1].partNumber : null;
            const xmlParams = {
                bucketName,
                objectKey,
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
        },
    ], function waterfallFinal(err, xml) {
        return callback(err, xml);
    });
}
