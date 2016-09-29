import async from 'async';
import querystring from 'querystring';

import constants from '../../constants';
import services from '../services';
import escapeForXML from '../utilities/escapeForXML';
import { errors } from 'arsenal';

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

function buildXML(xmlParams, xml) {
    xmlParams.forEach(param => {
        if (param.value !== undefined) {
            xml.push(`<${param.tag}>${param.value}</${param.tag}>`);
        } else {
            if (param.tag !== 'NextPartNumberMarker' &&
                param.tag !== 'PartNumberMarker') {
                xml.push(`<${param.tag}/>`);
            }
        }
    });
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
    let maxParts = Number.parseInt(request.query['max-parts'], 10) ?
        Number.parseInt(request.query['max-parts'], 10) : 1000;
    if (maxParts < 0) {
        return callback(errors.InvalidArgument);
    }
    if (maxParts > constants.listingHardLimit) {
        maxParts = constants.listingHardLimit;
    }
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

    let splitter = constants.splitter;

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
            // BACKWARD: Remove to remove the old splitter
            if (mpuBucket.getMdBucketModelVersion() < 2) {
                splitter = constants.oldSplitter;
            }
            const getPartsParams = {
                uploadId,
                mpuBucketName: mpuBucket.getName(),
                maxParts,
                partNumberMarker,
                log,
                splitter,
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
            } else {
                objectKey = escapeForXML(objectKey);
            }
            const isTruncated = storedParts.IsTruncated;
            const splitterLen = splitter.length;
            const partListing = storedParts.Contents.map(item => {
                // key form:
                // - {uploadId}
                // - {splitter}
                // - {partNumber}
                const index = item.key.lastIndexOf(splitter);
                return {
                    partNumber: item.key.substring(index + splitterLen),
                    lastModified: item.value.LastModified,
                    ETag: item.value.ETag,
                    size: item.value.Size,
                };
            });
            const lastPartShown = partListing.length > 0 ?
                partListing[partListing.length - 1].partNumber : undefined;

            const xml = [];
            xml.push(
                '<?xml version="1.0" encoding="UTF-8"?>',
                '<ListPartsResult xmlns="http://s3.amazonaws.com/doc/' +
                    '2006-03-01/">'
            );
            buildXML([
                { tag: 'Bucket', value: bucketName },
                { tag: 'Key', value: objectKey },
                { tag: 'UploadId', value: uploadId },
            ], xml);
            xml.push('<Initiator>');
            buildXML([
                { tag: 'ID', value: mpuOverviewArray[4] },
                { tag: 'DisplayName', value: mpuOverviewArray[5] },
            ], xml);
            xml.push('</Initiator>');
            xml.push('<Owner>');
            buildXML([
                { tag: 'ID', value: mpuOverviewArray[6] },
                { tag: 'DisplayName', value: mpuOverviewArray[7] },
            ], xml);
            xml.push('</Owner>');
            buildXML([
                { tag: 'StorageClass', value: mpuOverviewArray[8] },
                { tag: 'PartNumberMarker', value: partNumberMarker ||
                    undefined },
                { tag: 'NextPartNumberMarker', value: isTruncated ?
                    lastPartShown : undefined }, // print only if it's truncated
                { tag: 'MaxParts', value: maxParts },
                { tag: 'IsTruncated', value: isTruncated ? 'true' : 'false' },
            ], xml);

            partListing.forEach(part => {
                xml.push('<Part>');
                buildXML([
                    { tag: 'PartNumber', value: part.partNumber },
                    { tag: 'LastModified', value: part.lastModified },
                    { tag: 'ETag', value: part.ETag },
                    { tag: 'Size', value: part.size },
                ], xml);
                xml.push('</Part>');
            });
            xml.push('</ListPartsResult>');
            next(null, xml.join(''));
        },
    ], callback);
    return undefined;
}
