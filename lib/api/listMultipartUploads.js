import async from 'async';
import escapeForXML from '../utilities/escapeForXML';
import querystring from 'querystring';

import constants from '../../constants';
import services from '../services';
import { errors } from 'arsenal';

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


/**
 * _convertToXml - Converts the `xmlParams` object (defined in
 * `listMultipartUploads()`) to an XML DOM string
 * @param {object} xmlParams - The `xmlParams` object defined in
 * `listMultipartUploads()`
 * @return {string} xml.join('') - The XML DOM string
 */
const _convertToXml = xmlParams => {
    const xml = [];
    const l = xmlParams.list;

    xml.push('<?xml version="1.0" encoding="UTF-8"?>',
             '<ListMultipartUploadsResult ' +
                'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
             `<Bucket>${xmlParams.bucketName}</Bucket>`
    );

    // For certain XML elements, if it is `undefined`, AWS returns either an
    // empty tag or does not include it. Hence the `optional` key in the params.
    const params = [
        { tag: 'KeyMarker', value: escapeForXML(xmlParams.keyMarker) },
        { tag: 'UploadIdMarker', value: xmlParams.uploadIdMarker },
        { tag: 'NextKeyMarker', value: escapeForXML(l.NextKeyMarker),
            optional: true },
        { tag: 'NextUploadIdMarker', value: l.NextUploadIdMarker,
            optional: true },
        { tag: 'Delimiter', value: escapeForXML(l.Delimiter), optional: true },
        { tag: 'Prefix', value: escapeForXML(xmlParams.prefix),
            optional: true },
    ];

    params.forEach(param => {
        if (param.value) {
            xml.push(`<${param.tag}>${param.value}</${param.tag}>`);
        } else if (!param.optional) {
            xml.push(`<${param.tag}></${param.tag}>`);
        }
    });

    xml.push(`<MaxUploads>${l.MaxKeys}</MaxUploads>`,
             `<IsTruncated>${l.IsTruncated}</IsTruncated>`
    );

    l.Uploads.forEach(upload => {
        const val = upload.value;
        let key = upload.key;
        if (xmlParams.encoding === 'url') {
            key = querystring.escape(key);
        }

        xml.push('<Upload>',
                 `<Key>${escapeForXML(key)}</Key>`,
                 `<UploadId>${val.UploadId}</UploadId>`,
                 '<Initiator>',
                 `<ID>${val.Initiator.ID}</ID>`,
                 `<DisplayName>${escapeForXML(val.Initiator.DisplayName)}` +
                    '</DisplayName>',
                 '</Initiator>',
                 '<Owner>',
                 `<ID>${val.Owner.ID}</ID>`,
                 `<DisplayName>${escapeForXML(val.Owner.DisplayName)}` +
                    '</DisplayName>',
                 '</Owner>',
                 `<StorageClass>${val.StorageClass}</StorageClass>`,
                 `<Initiated>${val.Initiated}</Initiated>`,
                 '</Upload>'
        );
    });

    l.CommonPrefixes.forEach(prefix => {
        xml.push('<CommonPrefixes>',
                 `<Prefix>${escapeForXML(prefix)}</Prefix>`,
                 '</CommonPrefixes>'
        );
    });

    xml.push('</ListMultipartUploadsResult>');

    return xml.join('');
};

/**
 * list multipart uploads - Return list of open multipart uploads
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to respond to http request
 *  with either error code or xml response body
 * @return {undefined}
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

    async.waterfall([
        function waterfall1(next) {
            // Check final destination bucket for authorization rather
            // than multipart upload bucket
            services.metadataValidateAuthorization(metadataValParams,
                (err, bucket) => next(err, bucket));
        },
        function getMPUBucket(bucket, next) {
            services.getMPUBucket(bucket, bucketName, log,
                (err, mpuBucket) => next(err, mpuBucket));
        },
        function waterfall2(mpuBucket, next) {
            let splitter = constants.splitter;
            // BACKWARD: Remove to remove the old splitter
            if (mpuBucket.getMdBucketModelVersion() < 2) {
                splitter = constants.oldSplitter;
            }
            let maxUploads = query['max-uploads'] !== undefined ?
                Number.parseInt(query['max-uploads'], 10) : 1000;
            if (maxUploads < 0) {
                return callback(errors.InvalidArgument);
            }
            if (maxUploads > constants.listingHardLimit) {
                maxUploads = constants.listingHardLimit;
            }
            const listingParams = {
                delimiter: query.delimiter,
                keyMarker: query['key-marker'],
                uploadIdMarker: query['upload-id-marker'],
                maxKeys: maxUploads,
                prefix: `overview${splitter}${prefix}`,
                queryPrefixLength: prefix.length,
                listingType: 'multipartuploads',
                splitter,
            };
            services.getMultipartUploadListing(mpuBucketName, listingParams,
                log, next);
            return undefined;
        },
    ], (err, list) => {
        if (err) {
            return callback(err);
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
