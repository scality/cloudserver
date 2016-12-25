import querystring from 'querystring';
import indexClient from '../indexClient/indexClient';
import constants from '../../constants';

import services from '../services';
import escapeForXML from '../utilities/escapeForXML';
import { pushMetric } from '../utapi/utilities';
import { errors } from 'arsenal';

//	Sample XML response:
/*	<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>example-bucket</Name>
  <Prefix></Prefix>
  <Marker></Marker>
  <MaxKeys>1000</MaxKeys>
  <Delimiter>/</Delimiter>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>sample.jpg</Key>
    <LastModified>2011-02-26T01:56:20.000Z</LastModified>
    <ETag>&quot;bf1d737a4d46a19f3bced6905cc8b902&quot;</ETag>
    <Size>142863</Size>
    <Owner>
      <ID>canonical-user-id</ID>
      <DisplayName>display-name</DisplayName>
    </Owner>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
  <CommonPrefixes>
    <Prefix>photos/</Prefix>
  </CommonPrefixes>
</ListBucketResult>*/

/**
 * bucketGet - Return list of objects in bucket
 * @param  {AuthInfo} authInfo - Instance of AuthInfo class with
 *                               requester's info
 * @param  {object} request - http request object
 * @param  {function} log - Werelogs request logger
 * @param  {function} callback - callback to respond to http request
 *  with either error code or xml response body
 * @return {undefined}
 */
export default function bucketGet(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketGet' });
    const params = request.query;
    const bucketName = request.bucketName;
    const encoding = params['encoding-type'];
    if (encoding !== undefined && encoding !== 'url') {
        return callback(errors.InvalidArgument.customizeDescription('Invalid ' +
            'Encoding Method specified in Request'));
    }
    const requestMaxKeys = params['max-keys'] ?
        Number.parseInt(params['max-keys'], 10) : 1000;
    if (Number.isNaN(requestMaxKeys) || requestMaxKeys < 0) {
        return callback(errors.InvalidArgument);
    }
    // AWS only returns 1000 keys even if max keys are greater.
    // Max keys stated in response xml can be greater than actual
    // keys returned.
    const actualMaxKeys = Math.min(constants.listingHardLimit, requestMaxKeys);

    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: 'bucketGet',
        log,
    };
    const listParams = {
        maxKeys: actualMaxKeys,
        delimiter: params.delimiter,
        marker: params.marker,
        prefix: params.prefix,
        query: indexClient.processQueryHeader(request.headers.query),
    };

    services.metadataValidateAuthorization(metadataValParams, err => {
        if (err) {
            log.debug('error processing request', { error: err });
            return callback(err);
        }
        return services.getObjectListing(bucketName, listParams, log,
        (err, list) => {
            if (err) {
                log.debug('error processing request', { error: err });
                return callback(err);
            }
            listParams.prefix = params.prefix;
            const xml = [];
            xml.push(
                '<?xml version="1.0" encoding="UTF-8"?>',
                '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/' +
                    '2006-03-01/">',
                `<Name>${bucketName}</Name>`
            );
            const isTruncated = list.IsTruncated ? 'true' : 'false';
            const xmlParams = [
                { tag: 'Prefix', value: listParams.prefix },
                { tag: 'NextMarker', value: list.NextMarker },
                { tag: 'Marker', value: listParams.marker },
                { tag: 'MaxKeys', value: requestMaxKeys },
                { tag: 'Delimiter', value: listParams.delimiter },
                { tag: 'EncodingType', value: encoding },
                { tag: 'IsTruncated', value: isTruncated },
            ];

            xmlParams.forEach(param => {
                if (param.value) {
                    xml.push(`<${param.tag}>${param.value}</${param.tag}>`);
                } else if (param.tag !== 'NextMarker' &&
                           param.tag !== 'EncodingType' &&
                           param.tag !== 'Delimiter') {
                    xml.push(`<${param.tag}/>`);
                }
            });

            list.Contents.forEach(item => {
                const v = item.value;
                const objectKey = encoding === 'url' ?
                    querystring.escape(item.key) : escapeForXML(item.key);

                xml.push(
                    '<Contents>',
                    `<Key>${objectKey}</Key>`,
                    `<LastModified>${v.LastModified}</LastModified>`,
                    `<ETag>&quot;${v.ETag}&quot;</ETag>`,
                    `<Size>${v.Size}</Size>`,
                    '<Owner>',
                    `<ID>${v.Owner.ID}</ID>`,
                    `<DisplayName>${v.Owner.DisplayName}</DisplayName>`,
                    '</Owner>',
                    `<StorageClass>${v.StorageClass}</StorageClass>`,
                    '</Contents>'
                );
            });
            list.CommonPrefixes.forEach(item => {
                xml.push(
                    `<CommonPrefixes><Prefix>${item}</Prefix></CommonPrefixes>`
                );
            });
            xml.push('</ListBucketResult>');
            pushMetric('listBucket', log, {
                bucket: bucketName,
            });
            return callback(null, xml.join(''));
        });
    });
    return undefined;
}
