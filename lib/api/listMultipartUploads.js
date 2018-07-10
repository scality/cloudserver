const async = require('async');

const { errors, s3middleware } = require('arsenal');
const convertToXml = s3middleware.convertToXml;

const constants = require('../../constants');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const services = require('../services');
const { metadataValidateBucket } = require('../metadata/metadataUtils');
const { pushMetric } = require('../utapi/utilities');
const monitoring = require('../utilities/monitoringHandler');

//  Sample XML response:
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
 * list multipart uploads - Return list of open multipart uploads
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to respond to http request
 *  with either error code or xml response body
 * @return {undefined}
 */
function listMultipartUploads(authInfo, request, log, callback) {
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
    };

    async.waterfall([
        function waterfall1(next) {
            // Check final destination bucket for authorization rather
            // than multipart upload bucket
            metadataValidateBucket(metadataValParams, log,
                (err, bucket) => next(err, bucket));
        },
        function getMPUBucket(bucket, next) {
            services.getMPUBucket(bucket, bucketName, log,
                (err, mpuBucket) => next(err, bucket, mpuBucket));
        },
        function waterfall2(bucket, mpuBucket, next) {
            let splitter = constants.splitter;
            // BACKWARD: Remove to remove the old splitter
            if (mpuBucket.getMdBucketModelVersion() < 2) {
                splitter = constants.oldSplitter;
            }
            let maxUploads = query['max-uploads'] !== undefined ?
                Number.parseInt(query['max-uploads'], 10) : 1000;
            if (maxUploads < 0) {
                monitoring.promMetrics('GET', bucketName, 400,
                    'listMultipartUploads');
                return callback(errors.InvalidArgument, bucket);
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
                listingType: 'MPU',
                splitter,
            };
            services.getMultipartUploadListing(mpuBucketName, listingParams,
                log, (err, list) => next(err, bucket, list));
            return undefined;
        },
    ], (err, bucket, list) => {
        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (err) {
            monitoring.promMetrics('GET', bucketName, err.code,
                'listMultipartUploads');
            return callback(err, null, corsHeaders);
        }
        const xmlParams = {
            bucketName,
            encoding,
            list,
            prefix: query.prefix,
            keyMarker: query['key-marker'],
            uploadIdMarker: query['upload-id-marker'],
        };
        const xml = convertToXml('listMultipartUploads', xmlParams);
        pushMetric('listMultipartUploads', log, {
            authInfo,
            bucket: bucketName,
        });
        monitoring.promMetrics(
            'GET', bucketName, '200', 'listMultipartUploads');
        return callback(null, xml, corsHeaders);
    });
}

module.exports = listMultipartUploads;
