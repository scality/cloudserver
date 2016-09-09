import querystring from 'querystring';
import constants from '../../constants';

import services from '../services';
import escapeForXML from '../utilities/escapeForXML';
import { errors, versioning } from 'arsenal';

const VSUtils = versioning.VersioningUtils;

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

function formatXML(bucketName, listParams, encoding, list) {
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
        { tag: 'MaxKeys', value: listParams.maxKeys },
        { tag: 'Delimiter', value: listParams.delimiter },
        { tag: 'IsTruncated', value: isTruncated },
    ];

    xmlParams.forEach(param => {
        if (param.value) {
            xml.push(`<${param.tag}>${param.value}</${param.tag}>`);
        } else {
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
    return xml.join('');
}

// Sample XML response for versioning:
// http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGETVersion.html
/*
<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01">
    <Name>bucket</Name>
    <Prefix>my</Prefix>
    <KeyMarker/>
    <VersionIdMarker/>
    <MaxKeys>5</MaxKeys>
    <IsTruncated>false</IsTruncated>
    <Version>
        <Key>my-image.jpg</Key>
        <VersionId>3/L4kqtJl40Nr8X8gdRQBpUMLUo</VersionId>
        <IsLatest>true</IsLatest>
        <LastModified>2009-10-12T17:50:30.000Z</LastModified>
        <ETag>&quot;fba9dede5f27731c9771645a39863328&quot;</ETag>
        <Size>434234</Size>
        <StorageClass>STANDARD</StorageClass>
        <Owner>
            <ID>canonical-user-id</ID>
            <DisplayName>mtd@amazon.com</DisplayName>
        </Owner>
    </Version>
    <DeleteMarker>
        <Key>my-image.jpg</Key>
        <VersionId>03jpff543dhffds434rfdsFDN943fdsFkdmqnh892</VersionId>
        <IsLatest>true</IsLatest>
        <LastModified>2009-11-12T17:50:30.000Z</LastModified>
        <Owner>
            <ID>canonical-user-id</ID>
            <DisplayName>mtd@amazon.com</DisplayName>
        </Owner>
    </DeleteMarker>
</ListVersionsResult>
*/

function formatVersionsXML(bucketName, listParams, encoding, list) {
    const xml = [];
    xml.push(
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/' +
            '2006-03-01/">',
        `<Name>${bucketName}</Name>`
    );

    const xmlParams = [
        { tag: 'Prefix', value: listParams.prefix },
        { tag: 'MaxKeys', value: listParams.maxKeys },
        { tag: 'KeyMarker', value: listParams.keyMarker },
        { tag: 'VersionIdMarker', value: listParams.versionIdMarker },
    ];

    xmlParams.forEach(param => {
        if (param.value) {
            xml.push(`<${param.tag}>${param.value}</${param.tag}>`);
        } else {
            xml.push(`<${param.tag}/>`);
        }
    });

    const latestVersions = list.LatestVersions;

    list.Contents.forEach(item => {
        const key = encoding === 'url' ?
            querystring.escape(item.key) : escapeForXML(item.key);
        const v = VSUtils.decodeVersion(item.value);
        const size = v['content-length'];
        const ETag = v['content-md5'];
        const lastModified = v['last-modified'];
        const owner = {
            DisplayName: v['owner-display-name'],
            ID: v['owner-id'],
        };
        const storageClass = v['x-amz-storage-class'];

        const versionId = VSUtils.getts(v);
        const isLatest = latestVersions && versionId === latestVersions[key];
        if (VSUtils.isDeleteMarker(v)) {
            xml.push(
                '<DeleteMarker>',
                `<Key>${key}</Key>`,
                `<VersionId>${versionId}</VersionId>`,
                `<IsLatest>${isLatest}</IsLatest>`,
                `<LastModified>${lastModified}</LastModified>`,
                '<Owner>',
                `<ID>${owner.ID}</ID>`,
                `<DisplayName>${owner.DisplayName}</DisplayName>`,
                '</Owner>',
                '</DeleteMarker>'
            );
        } else {
            xml.push(
                '<Version>',
                `<Key>${key}</Key>`,
                `<VersionId>${versionId}</VersionId>`,
                `<IsLatest>${isLatest}</IsLatest>`,
                `<LastModified>${lastModified}</LastModified>`,
                `<ETag>${ETag}</ETag>`,
                `<Size>${size}</Size>`,
                '<Owner>',
                `<ID>${owner.ID}</ID>`,
                `<DisplayName>${owner.DisplayName}</DisplayName>`,
                '</Owner>',
                `<StorageClass>${storageClass}</StorageClass>`,
                '</Version>'
            );
        }
    });
    list.CommonPrefixes.forEach(item => {
        xml.push(
            `<CommonPrefixes><Prefix>${item}</Prefix></CommonPrefixes>`
        );
    });
    xml.push('</ListVersionsResult>');
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
    let maxKeys = params['max-keys'] ?
        Number.parseInt(params['max-keys'], 10) : 1000;
    if (maxKeys < 0) {
        return callback(errors.InvalidArgument);
    }
    if (maxKeys > constants.listingHardLimit) {
        maxKeys = constants.listingHardLimit;
    }
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: 'bucketGet',
        log,
    };

    services.metadataValidateAuthorization(metadataValParams, (err, bucket) => {
        if (err) {
            log.debug('error processing request', { error: err });
            return callback(err);
        }
        const listParams = {
            maxKeys,
            delimiter: params.delimiter,
            marker: params.marker,
            prefix: params.prefix,
            versioning: bucket.isVersioningOn(),
            versions: params.versions === 'true' || params.versions === '',
        };
        return services.getObjectListing(bucketName, listParams, log,
        (err, list) => {
            if (err) {
                log.debug('error processing request', { error: err });
                return callback(err);
            }
            log.info('received list', list);
            const xml = listParams.versions ?
                formatVersionsXML(bucketName, listParams, encoding, list) :
                formatXML(bucketName, listParams, encoding, list);
            return callback(null, xml);
        });
    });
    return undefined;
}
