import async from 'async';
import xml from 'xml';

import constants from '../../constants';
import services from '../services';

const splitter = constants.splitter;

//	Sample XML response:
/*
<?xml version='1.0' encoding='UTF-8'?>
<ListMultipartUploadsResult xmlns='http://s3.amazonaws.com/doc/2006-03-01/'>
  <Bucket>bucket</Bucket>
  <KeyMarker></KeyMarker>
  <UploadIdMarker></UploadIdMarker>
  <NextKeyMarker>my-movie.m2ts</NextKeyMarker>
  <NextUploadIdMarker>YW55IGlkZWEgd2h5IGVsdmlu
  ZydzIHVwbG9hZCBmYWlsZWQ</NextUploadIdMarker>
  <MaxUploads>3</MaxUploads>
  <IsTruncated>true</IsTruncated>
  <Upload>
    <Key>my-divisor</Key>
    <UploadId>XMgbGlrZSBlbHZpbmcncyBub3QgaGF2aW5nIG11Y2ggbHVjaw</UploadId>
    <Initiator>
      <ID>arn:aws:iam::111122223333:user/
      user1-11111a31-17b5-4fb7-9df5-b111111f13de</ID>
      <DisplayName>user1-11111a31-17b5-4fb7-9df5-b111111f13de</DisplayName>
    </Initiator>
    <Owner>
      <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
      <DisplayName>OwnerDisplayName</DisplayName>
    </Owner>
    <StorageClass>STANDARD</StorageClass>
    <Initiated>2010-11-10T20:48:33.000Z</Initiated>
  </Upload>
  <Upload>
    <Key>my-movie.m2ts</Key>
    <UploadId>VXBsb2FkIElEIGZvciBlbHZpbmcn
    cyBteS1tb3ZpZS5tMnRzIHVwbG9hZA</UploadId>
    <Initiator>
      <ID>b1d16700c70b0b05597d7acd6a3f92be</ID>
      <DisplayName>InitiatorDisplayName</DisplayName>
    </Initiator>
    <Owner>
      <ID>b1d16700c70b0b05597d7acd6a3f92be</ID>
      <DisplayName>OwnerDisplayName</DisplayName>
    </Owner>
    <StorageClass>STANDARD</StorageClass>
    <Initiated>2010-11-10T20:48:33.000Z</Initiated>
  </Upload>
  <Upload>
    <Key>my-movie.m2ts</Key>
    <UploadId>YW55IGlkZWEgd2h5IGVsdmluZydzIHVwbG9hZCBmYWlsZWQ</UploadId>
    <Initiator>
      <ID>arn:aws:iam::444455556666:
      user/user1-22222a31-17b5-4fb7-9df5-b222222f13de</ID>
      <DisplayName>user1-22222a31-17b5-4fb7-9df5-b222222f13de</DisplayName>
    </Initiator>
    <Owner>
      <ID>b1d16700c70b0b05597d7acd6a3f92be</ID>
      <DisplayName>OwnerDisplayName</DisplayName>
    </Owner>
    <StorageClass>STANDARD</StorageClass>
    <Initiated>2010-11-10T20:49:33.000Z</Initiated>
  </Upload>
</ListMultipartUploadsResult>
 */

 /*
    Construct JSON in proper format to be converted to XML
    to be returned to client
 */
function _constructJSON(xmlParams) {
    const json = xmlParams.list;
    let listMPUResultArray = [
			{_attr: {
    xmlns: `http://s3.amazonaws.com/doc/2006-03-01/`
			}},
			{Bucket: xmlParams.bucketname},
            {KeyMarker: xmlParams.keyMarker},
            {UploadIdMarker: xmlParams.uploadIdMarker},
            {NextKeyMarker: json.NextKeyMarker},
            {NextUploadIdMarker: json.NextUploadIdMarker},
            {Delimiter: json.Delimiter},
            {Prefix: xmlParams.prefix},
            {MaxUploads: json.MaxKeys},
            {IsTruncated: json.IsTruncated}
    ];

    const contents = json.Uploads.map((upload) => {
        let key = upload.Key;
        if (xmlParams.encoding === 'url') {
            key = encodeURIComponent(key);
        }
        return {
            Upload: [
                {Key: key},
                {UploadId: upload.UploadId},
                {Initiator: [
                    {ID: upload.Initiator.ID},
                    {DisplayName: upload.Initiator.DisplayName}
                ]},
                {Owner: [
                    {ID: upload.Owner.ID},
                    {DisplayName: upload.Owner.DisplayName}
                ]},
                {StorageClass: upload.StorageClass},
                // Initiated date was converted to take out "-"
                // and "." to prevent routing errors.  Need
                // to replace back.
                {Initiated: upload.Initiated}
            ]
        };
    });

    if (contents.length > 0) {
        listMPUResultArray = listMPUResultArray.concat(contents);
    }

    const commonPrefixes = json.CommonPrefixes.map((item) => {
        return {
            Prefix: item,
        };
    });

    if (commonPrefixes.length > 0) {
        const commonPrefixesObject = {CommonPrefixes: commonPrefixes};
        listMPUResultArray.push(commonPrefixesObject);
    }

    const constructedJSON = {
        ListMultipartUploadsResult: listMPUResultArray
    };

    return constructedJSON;
}


function _convertToXml(xmlParams) {
    const constructedJSON = _constructJSON(xmlParams);
    return xml(constructedJSON, { declaration: { encoding: 'UTF-8' }});
}

/**
 * list multipart uploads - Return list of open multipart uploads
 * @param  {AuthInfo} Instance of AuthInfo class with requester's info
 * @param  {object} request - http request object
 * @param  {object} log - Werelogs logger
 * @param  {function} callback - callback to respond to http request
 *  with either error code or xml response body
 */
export default function listMultipartUploads(authInfo,
    request, log, callback) {
    log.debug('processing request', { method: 'listMultipartUploads' });
    const query = request.query;
    const bucketName = request.bucketName;
    const mpuBucketName = `${constants.mpuBucketPrefix}${bucketName}`;
    const encoding = query['encoding-type'];
    const prefix = query.prefix ? query.prefix : '';
    const metadataValParams = {
        authInfo,
        bucketName,
        // AWS docs state that the bucket owner and anyone with
        // s3:ListBucketMultipartUploads rights under the bucket policies
        // have access. They do not address ACLs at all.  It seems inconsisent
        // that someone who has read access on a bucket would not be able
        // to list the multipart uploads so we have provided here that
        // the authorization to list multipart uploads is the same
        // as listing objects in a bucket.
        requestType: 'bucketGet',
        log,
    };
    const listingParams = {
        delimiter: query.delimiter,
        keyMarker: query['key-marker'],
        uploadIdMarker: query['upload-id-marker'],
        maxKeys: query['max-uploads'],
        prefix: `overview${splitter}${prefix}`,
        queryPrefixLength: prefix.length,
    };

    async.waterfall([
        function waterfall1(next) {
            // Check final destination bucket for authorization rather
            // than multipart upload bucket
            services.metadataValidateAuthorization(metadataValParams, (err) => {
                if (err) {
                    return next(err);
                }
                next();
            });
        },
        function waterfall2(next) {
            services.getMultipartUploadListing(mpuBucketName,
                listingParams, log, next);
        }
    ], function waterfallFinal(err, list) {
        if (err) {
            return callback(err, null);
        }
        const xmlParams = {
            bucketName,
            encoding,
            list,
            prefix: query.prefix,
            keyMarker: query['key-marker'],
            uploadIdMarker: query['upload-id-marker'],
        };
        const xml = _convertToXml(xmlParams);
        return callback(null, xml);
    });
}
