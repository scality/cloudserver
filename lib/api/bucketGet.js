import querystring from 'querystring';
import constants from '../../constants';

import services from '../services';
import collectCorsHeaders from '../utilities/collectCorsHeaders';
import escapeForXML from '../utilities/escapeForXML';
import { pushMetric } from '../utapi/utilities';
import { errors, versioning } from 'arsenal';

const versionIdUtils = versioning.VersionID;

//	Sample XML response for GET bucket objects:
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

/* eslint-disable max-len */
// sample XML response for GET bucket object versions:
// http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGETVersion.html#RESTBucketGET_Examples
/*
<?xml version="1.0" encoding="UTF-8"?>

<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01">
    <Name>bucket</Name>
    <Prefix>my</Prefix>
    <KeyMarker/>
    <VersionIdMarker/>
    <MaxKeys>5</MaxKeys>
    <Delimiter>/</Delimiter>
    <NextKeyMarker>>my-second-image.jpg</NextKeyMarker>
    <NextVersionIdMarker>03jpff543dhffds434rfdsFDN943fdsFkdmqnh892</NextVersionIdMarker>
    <IsTruncated>true</IsTruncated>
    <Version>
        <Key>my-image.jpg</Key>
        <VersionId>3/L4kqtJl40Nr8X8gdRQBpUMLUo</VersionId>
        <IsLatest>true</IsLatest>
         <LastModified>2009-10-12T17:50:30.000Z</LastModified>
        <ETag>&quot;fba9dede5f27731c9771645a39863328&quot;</ETag>
        <Size>434234</Size>
        <StorageClass>STANDARD</StorageClass>
        <Owner>
            <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
            <DisplayName>mtd@amazon.com</DisplayName>
        </Owner>
    </Version>
    <DeleteMarker>
        <Key>my-second-image.jpg</Key>
        <VersionId>03jpff543dhffds434rfdsFDN943fdsFkdmqnh892</VersionId>
        <IsLatest>true</IsLatest>
        <LastModified>2009-11-12T17:50:30.000Z</LastModified>
        <Owner>
            <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
            <DisplayName>mtd@amazon.com</DisplayName>
        </Owner>
    </DeleteMarker>
    <CommonPrefixes>
        <Prefix>photos/</Prefix>
    </CommonPrefixes>
</ListVersionsResult>
*/
/* eslint-enable max-len */

function processVersions(bucketName, listParams, list) {
    const xml = [];
    xml.push(
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
        '<Name>', bucketName, '</Name>'
    );
    const isTruncated = list.IsTruncated ? 'true' : 'false';
    const xmlParams = [
        { tag: 'Prefix', value: listParams.prefix },
        { tag: 'KeyMarker', value: listParams.keyMarker },
        { tag: 'VersionIdMarker', value: listParams.versionIdMarker },
        { tag: 'NextKeyMarker', value: list.NextKeyMarker },
        { tag: 'NextVersionIdMarker', value: list.NextVersionIdMarker },
        { tag: 'MaxKeys', value: listParams.maxKeys },
        { tag: 'Delimiter', value: listParams.delimiter },
        { tag: 'EncodingType', value: listParams.encoding },
        { tag: 'IsTruncated', value: isTruncated },
    ];

    const escapeXmlFn = listParams.encoding === 'url' ?
        querystring.escape : escapeForXML;
    xmlParams.forEach(p => {
        if (p.value) {
            const val = p.tag !== 'NextVersionIdMarker' || p.value === 'null' ?
                p.value : versionIdUtils.encrypt(p.value);
            xml.push(`<${p.tag}>${escapeXmlFn(val)}</${p.tag}>`);
        }
    });
    let lastKey = listParams.keyMarker ?
        escapeXmlFn(listParams.keyMarker) : undefined;
    list.Versions.forEach(item => {
        const v = JSON.parse(item.value);
        const objectKey = escapeXmlFn(item.key);
        const isLatest = lastKey !== objectKey;
        lastKey = objectKey;
        xml.push(
            v.isDeleteMarker ? '<DeleteMarker>' : '<Version>',
            `<Key>${objectKey}</Key>`,
            '<VersionId>',
            (v.isNull || v.versionId === undefined) ?
                'null' : versionIdUtils.encrypt(v.versionId),
            '</VersionId>',
            `<IsLatest>${isLatest}</IsLatest>`,
            `<LastModified>${v['last-modified']}</LastModified>`,
            `<ETag>&quot;${v['content-md5']}&quot;</ETag>`,
            `<Size>${v['content-length']}</Size>`,
            '<Owner>',
            `<ID>${v['owner-id']}</ID>`,
            `<DisplayName>${v['owner-display-name']}</DisplayName>`,
            '</Owner>',
            `<StorageClass>${v['x-amz-storage-class']}</StorageClass>`,
            v.isDeleteMarker ? '</DeleteMarker>' : '</Version>'
        );
    });
    list.CommonPrefixes.forEach(item => {
        const val = escapeXmlFn(item);
        xml.push(`<CommonPrefixes><Prefix>${val}</Prefix></CommonPrefixes>`);
    });
    xml.push('</ListVersionsResult>');
    return xml.join('');
}

function processMasterVersions(bucketName, listParams, list) {
    const xml = [];
    xml.push(
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
        '<Name>', bucketName, '</Name>'
    );
    const isTruncated = list.IsTruncated ? 'true' : 'false';
    const xmlParams = [
        { tag: 'Prefix', value: listParams.prefix || '' },
        { tag: 'Marker', value: listParams.marker || '' },
        { tag: 'NextMarker', value: list.NextMarker },
        { tag: 'MaxKeys', value: listParams.maxKeys },
        { tag: 'Delimiter', value: listParams.delimiter },
        { tag: 'EncodingType', value: listParams.encoding },
        { tag: 'IsTruncated', value: isTruncated },
    ];

    const escapeXmlFn = listParams.encoding === 'url' ?
        querystring.escape : escapeForXML;
    xmlParams.forEach(p => {
        if (p.value) {
            xml.push(`<${p.tag}>${escapeXmlFn(p.value)}</${p.tag}>`);
        } else if (p.tag !== 'NextMarker' &&
                p.tag !== 'EncodingType' &&
                p.tag !== 'Delimiter') {
            xml.push(`<${p.tag}/>`);
        }
    });

    list.Contents.forEach(item => {
        const v = item.value;
        if (v.isDeleteMarker) {
            return null;
        }
        const objectKey = escapeXmlFn(item.key);
        return xml.push(
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
        const val = escapeXmlFn(item);
        xml.push(`<CommonPrefixes><Prefix>${val}</Prefix></CommonPrefixes>`);
    });
    xml.push('</ListBucketResult>');
    return xml.join('');
}

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
        listingType: 'DelimiterMaster',
        maxKeys: actualMaxKeys,
        delimiter: params.delimiter,
        marker: params.marker,
        prefix: params.prefix,
    };

    services.metadataValidateAuthorization(metadataValParams, (err, bucket) => {
        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (err) {
            log.debug('error processing request', { error: err });
            return callback(err, null, corsHeaders);
        }
        if (params.versions !== undefined) {
            listParams.listingType = 'DelimiterVersions';
            delete listParams.marker;
            listParams.keyMarker = params['key-marker'];
            listParams.versionIdMarker = params['version-id-marker'] ?
                versionIdUtils.decrypt(params['version-id-marker']) : undefined;
        }
        return services.getObjectListing(bucketName, listParams, log,
        (err, list) => {
            if (err) {
                log.debug('error processing request', { error: err });
                return callback(err, null, corsHeaders);
            }
            listParams.maxKeys = requestMaxKeys;
            listParams.encoding = encoding;
            let res = undefined;
            if (listParams.listingType === 'DelimiterVersions') {
                res = processVersions(bucketName, listParams, list);
            } else {
                res = processMasterVersions(bucketName, listParams, list);
            }
            pushMetric('listBucket', log, { authInfo, bucket: bucketName });
            return callback(null, res, corsHeaders);
        });
    });
    return undefined;
}
