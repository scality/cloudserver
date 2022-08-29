const querystring = require('querystring');
const { errors, versioning, s3middleware } = require('arsenal');
const constants = require('../../constants');
const services = require('../services');
const { metadataValidateBucket } = require('../metadata/metadataUtils');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const escapeForXml = s3middleware.escapeForXml;
const { pushMetric } = require('../utapi/utilities');
const versionIdUtils = versioning.VersionID;
const monitoring = require('../utilities/monitoringHandler');
const { generateToken, decryptToken }
    = require('../api/apiUtils/object/continueToken');

// do not url encode the continuation tokens
const skipUrlEncoding = new Set([
    'ContinuationToken',
    'NextContinuationToken',
]);

/* Sample XML response for GET bucket objects V2:
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Name>example-bucket</Name>
    <Prefix/>
    <KeyCount>205</KeyCount>
    <MaxKeys>1000</MaxKeys>
    <IsTruncated>false</IsTruncated>
    <Contents>
        <Key>my-image.jpg</Key>
        <LastModified>2009-10-12T17:50:30.000Z</LastModified>
        <ETag>&quot;fba9dede5f27731c9771645a39863328&quot;</ETag>
        <Size>434234</Size>
        <StorageClass>STANDARD</StorageClass>
    </Contents>
</ListBucketResult>
*/

/*	Sample XML response for GET bucket objects:
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
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
</ListBucketResult>
*/

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
        querystring.escape : escapeForXml;
    xmlParams.forEach(p => {
        if (p.value) {
            const val = p.tag !== 'NextVersionIdMarker' || p.value === 'null' ?
                p.value : versionIdUtils.encode(p.value);
            xml.push(`<${p.tag}>${escapeXmlFn(val)}</${p.tag}>`);
        }
    });
    let lastKey = listParams.keyMarker ?
        escapeXmlFn(listParams.keyMarker) : undefined;
    list.Versions.forEach(item => {
        const v = item.value;
        const objectKey = escapeXmlFn(item.key);
        const isLatest = lastKey !== objectKey;
        lastKey = objectKey;
        xml.push(
            v.IsDeleteMarker ? '<DeleteMarker>' : '<Version>',
            `<Key>${objectKey}</Key>`,
            '<VersionId>',
            (v.IsNull || v.VersionId === undefined) ?
                'null' : versionIdUtils.encode(v.VersionId),
            '</VersionId>',
            `<IsLatest>${isLatest}</IsLatest>`,
            `<LastModified>${v.LastModified}</LastModified>`,
            `<ETag>&quot;${v.ETag}&quot;</ETag>`,
            `<Size>${v.Size}</Size>`,
            '<Owner>',
            `<ID>${v.Owner.ID}</ID>`,
            `<DisplayName>${v.Owner.DisplayName}</DisplayName>`,
            '</Owner>',
            `<StorageClass>${v.StorageClass}</StorageClass>`,
            v.IsDeleteMarker ? '</DeleteMarker>' : '</Version>'
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
        { tag: 'MaxKeys', value: listParams.maxKeys },
        { tag: 'Delimiter', value: listParams.delimiter },
        { tag: 'EncodingType', value: listParams.encoding },
        { tag: 'IsTruncated', value: isTruncated },
    ];

    if (listParams.v2) {
        xmlParams.push(
            { tag: 'StartAfter', value: listParams.startAfter || '' });
        xmlParams.push(
            { tag: 'FetchOwner', value: `${listParams.fetchOwner}` });
        xmlParams.push({
            tag: 'ContinuationToken',
            value: generateToken(listParams.continuationToken) || '',
        });
        xmlParams.push({
            tag: 'NextContinuationToken',
            value: generateToken(list.NextContinuationToken),
        });
        xmlParams.push({
            tag: 'KeyCount',
            value: list.Contents ? list.Contents.length : 0,
        });
    } else {
        xmlParams.push({ tag: 'Marker', value: listParams.marker || '' });
        xmlParams.push({ tag: 'NextMarker', value: list.NextMarker });
    }

    const escapeXmlFn = listParams.encoding === 'url' ?
        querystring.escape : escapeForXml;
    xmlParams.forEach(p => {
        if (p.value && skipUrlEncoding.has(p.tag)) {
            xml.push(`<${p.tag}>${p.value}</${p.tag}>`);
        } else if (p.value || p.tag === 'KeyCount') {
            xml.push(`<${p.tag}>${escapeXmlFn(p.value)}</${p.tag}>`);
        } else if (p.tag !== 'NextMarker' &&
                p.tag !== 'EncodingType' &&
                p.tag !== 'Delimiter' &&
                p.tag !== 'StartAfter' &&
                p.tag !== 'NextContinuationToken') {
            xml.push(`<${p.tag}/>`);
        }
    });

    list.Contents.forEach(item => {
        const v = item.value;
        if (v.isDeleteMarker) {
            return null;
        }
        const objectKey = escapeXmlFn(item.key);
        xml.push(
            '<Contents>',
            `<Key>${objectKey}</Key>`,
            `<LastModified>${v.LastModified}</LastModified>`,
            `<ETag>&quot;${v.ETag}&quot;</ETag>`,
            `<Size>${v.Size}</Size>`
        );
        if (!listParams.v2 || listParams.fetchOwner) {
            xml.push(
                '<Owner>',
                `<ID>${v.Owner.ID}</ID>`,
                `<DisplayName>${v.Owner.DisplayName}</DisplayName>`,
                '</Owner>'
            );
        }
        return xml.push(
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

function handleResult(listParams, requestMaxKeys, encoding, authInfo,
    bucketName, list, corsHeaders, log, callback) {
    // eslint-disable-next-line no-param-reassign
    listParams.maxKeys = requestMaxKeys;
    // eslint-disable-next-line no-param-reassign
    listParams.encoding = encoding;
    let res;
    if (listParams.listingType === 'DelimiterVersions') {
        res = processVersions(bucketName, listParams, list);
    } else {
        res = processMasterVersions(bucketName, listParams, list);
    }
    pushMetric('listBucket', log, { authInfo, bucket: bucketName });
    monitoring.promMetrics('GET', bucketName, '200', 'listBucket');
    return callback(null, res, corsHeaders);
}

/**
 * bucketGet - Return list of objects in bucket, supports v1 & v2
 * @param  {AuthInfo} authInfo - Instance of AuthInfo class with
 *                               requester's info
 * @param  {object} request - http request object
 * @param  {function} log - Werelogs request logger
 * @param  {function} callback - callback to respond to http request
 *  with either error code or xml response body
 * @return {undefined}
 */
function bucketGet(authInfo, request, log, callback) {
    const params = request.query;
    const bucketName = request.bucketName;
    const v2 = params['list-type'];
    if (v2 !== undefined && Number.parseInt(v2, 10) !== 2) {
        return callback(errors.InvalidArgument.customizeDescription('Invalid ' +
            'List Type specified in Request'));
    }
    if (v2) {
        log.addDefaultFields({
            action: 'ListObjectsV2',
        });
    } else if (params.versions !== undefined) {
        log.addDefaultFields({
            action: 'ListObjectVersions',
        });
    }
    log.debug('processing request', { method: 'bucketGet' });
    const encoding = params['encoding-type'];
    if (encoding !== undefined && encoding !== 'url') {
        monitoring.promMetrics(
            'GET', bucketName, 400, 'listBucket');
        return callback(errors.InvalidArgument.customizeDescription('Invalid ' +
            'Encoding Method specified in Request'));
    }
    const requestMaxKeys = params['max-keys'] ?
        Number.parseInt(params['max-keys'], 10) : 1000;
    if (Number.isNaN(requestMaxKeys) || requestMaxKeys < 0) {
        monitoring.promMetrics(
            'GET', bucketName, 400, 'listBucket');
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
        request,
    };
    const listParams = {
        listingType: 'DelimiterMaster',
        maxKeys: actualMaxKeys,
        prefix: params.prefix,
    };

    if (params.delimiter) {
        listParams.delimiter = params.delimiter;
    }

    if (v2) {
        listParams.v2 = true;
        listParams.startAfter = params['start-after'];
        listParams.continuationToken =
            decryptToken(params['continuation-token']);
        listParams.fetchOwner = params['fetch-owner'] === 'true';
    } else {
        listParams.marker = params.marker;
    }

    metadataValidateBucket(metadataValParams, log, (err, bucket) => {
        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (err) {
            log.debug('error processing request', { error: err });
            monitoring.promMetrics(
                'GET', bucketName, err.code, 'listBucket');
            return callback(err, null, corsHeaders);
        }
        if (params.versions !== undefined) {
            listParams.listingType = 'DelimiterVersions';
            delete listParams.marker;
            listParams.keyMarker = params['key-marker'];
            listParams.versionIdMarker = params['version-id-marker'] ?
                versionIdUtils.decode(params['version-id-marker']) : undefined;
        }
        if (!requestMaxKeys) {
            const emptyList = {
                CommonPrefixes: [],
                Contents: [],
                Versions: [],
                IsTruncated: false,
            };
            return handleResult(listParams, requestMaxKeys, encoding, authInfo,
                bucketName, emptyList, corsHeaders, log, callback);
        }
        return services.getObjectListing(bucketName, listParams, log,
        (err, list) => {
            if (err) {
                log.debug('error processing request', { error: err });
                monitoring.promMetrics(
                    'GET', bucketName, err.code, 'listBucket');
                return callback(err, null, corsHeaders);
            }
            return handleResult(listParams, requestMaxKeys, encoding, authInfo,
                bucketName, list, corsHeaders, log, callback);
        });
    });
    return undefined;
}

module.exports = {
    processVersions,
    processMasterVersions,
    bucketGet,
};
