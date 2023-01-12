const querystring = require('querystring');
const { errors, versioning, s3middleware } = require('arsenal');
const constants = require('../../../constants');
const services = require('../../services');
const { metadataValidateBucket } = require('../../metadata/metadataUtils');
const collectCorsHeaders = require('../../utilities/collectCorsHeaders');
const escapeForXml = s3middleware.escapeForXml;
const { pushMetric } = require('../../utapi/utilities');
const versionIdUtils = versioning.VersionID;
const monitoring = require('../../utilities/monitoringHandler');
const { generateToken, decryptToken }
    = require('../../api/apiUtils/object/continueToken');

/*	Sample XML response for GET bucket objects:
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>example-bucket</Name>
  <Prefix></Prefix>
  <KeyMarker></KeyMarker>
  <NextKeyMarker></NextKeyMarker>
  <DateMarker></DateMarker>
  <NextDateMarker></NextDateMarker>
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
    <TagSet>
        <Tag>
            <Key>string</Key>
            <Value>string</Value>
        </Tag>
        <Tag>
            <Key>string</Key>
            <Value>string</Value>
        </Tag>
    </TagSet>
  </Contents>
</ListBucketResult>
*/

function xmlTags(tags) {
    const xml = [];
    Object.entries(tags).forEach(([key, value]) =>
        xml.push(
            '<Tag>',
                `<Key>${key}</Key>`,
                `<Value>${value}</Value>`,
            '</Tag>'
        ));
    return xml;
}

/* eslint-enable max-len */
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
        { tag: 'EncodingType', value: listParams.encoding },
        { tag: 'IsTruncated', value: isTruncated },
        { tag: 'DateMarker', value: listParams.dateMarker },
        { tag: 'NextDateMarker', value: list.NextDateMarker },
        { tag: 'KeyMarker', value: listParams.keyMarker },
        { tag: 'NextKeyMarker', value: list.NextKeyMarker },
    ];

    const escapeXmlFn = listParams.encoding === 'url' ?
        querystring.escape : escapeForXml;
    
    xmlParams.forEach(p => {
        if (p.value) {
            xml.push(`<${p.tag}>${escapeXmlFn(p.value)}</${p.tag}>`);
        }
    });

    list.Contents.forEach(item => {
        const v = item.value;
        const objectKey = escapeXmlFn(item.key);
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
                `<TagSet> ${xmlTags(v.tags)} </TagSet>`,
            '</Contents>',
        );
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
    const res = processMasterVersions(bucketName, listParams, list);

    pushMetric('listLifecycleObjects', log, { authInfo, bucket: bucketName });
    monitoring.promMetrics('GET', bucketName, '200', 'listLifecycleObjects');
    return callback(null, res, corsHeaders);
}

/**
 * listLifecycleObjects - Return list of objects in bucket, supports v1 & v2
 * @param  {AuthInfo} authInfo - Instance of AuthInfo class with
 *                               requester's info
 * @param  {object} request - http request object
 * @param  {function} log - Werelogs request logger
 * @param  {function} callback - callback to respond to http request
 *  with either error code or xml response body
 * @return {undefined}
 */
function listLifecycleObjects(authInfo, request, log, callback) {
    const params = request.query;
    const bucketName = request.bucketName;
    // const v2 = params['list-type'];
    // if (v2 !== undefined && Number.parseInt(v2, 10) !== 2) {
    //     return callback(errors.InvalidArgument.customizeDescription('Invalid ' +
    //         'List Type specified in Request'));
    // }
    // if (v2) {
    //     log.addDefaultFields({
    //         action: 'ListObjectsV2',
    //     });
    // } else if (params.versions !== undefined) {
    //     log.addDefaultFields({
    //         action: 'ListObjectVersions',
    //     });
    // }
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
        requestType: 'listLifecycleObjects',
        request,
    };
    const listParams = {
        listingType: 'DelimiterLifecycle',
        maxKeys: actualMaxKeys,
        prefix: params.prefix,
        dateMarker: params['date-marker'],
        beforeDate: params['before-date'],
        keyMarker: params['key-marker'],
        // before: params.before,
    };

    // if (params.delimiter) {
    //     listParams.delimiter = params.delimiter;
    // }

    // if (v2) {
    //     listParams.v2 = true;
    //     listParams.startAfter = params['start-after'];
    //     listParams.continuationToken =
    //         decryptToken(params['continuation-token']);
    //     listParams.fetchOwner = params['fetch-owner'] === 'true';
    // } else {
    //     listParams.marker = params.marker;
    // }
    // listParams.marker = params.marker;
    const beforeDate = params['before-date'];
    const dateMarker = params['date-marker'];
    let filter = null

    if (beforeDate || dateMarker) {
        filter = {'value.last-modified': {}};
        if (beforeDate) {
            filter['value.last-modified']['$lt'] = beforeDate;
        }
        if (dateMarker) {
            filter['value.last-modified']['$gt'] = dateMarker;
        }
    }

    // listParams.mongifiedSearch = filter;

    metadataValidateBucket(metadataValParams, log, (err, bucket) => {
        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (err) {
            log.debug('error processing request', { error: err });
            monitoring.promMetrics(
                'GET', bucketName, err.code, 'listBucket');
            return callback(err, null, corsHeaders);
        }
        // if (params.versions !== undefined) {
        //     listParams.listingType = 'DelimiterVersions';
        //     delete listParams.marker;
        //     listParams.keyMarker = params['key-marker'];
        //     listParams.versionIdMarker = params['version-id-marker'] ?
        //         versionIdUtils.decode(params['version-id-marker']) : undefined;
        // }
        if (!requestMaxKeys) {
            const emptyList = {
                CommonPrefixes: [],
                Contents: [],
                IsTruncated: false,
            };
            return handleResult(listParams, requestMaxKeys, encoding, authInfo,
                bucketName, emptyList, corsHeaders, log, callback);
        }
        
        return services.getLifecycleObjectListing(bucketName, listParams, log,
        (err, list) => {
            if (err) {
                log.debug('error processing request', { error: err });
                monitoring.promMetrics(
                    'GET', bucketName, err.code, 'listLifecycleObjects');
                return callback(err, null, corsHeaders);
            }
            return handleResult(listParams, requestMaxKeys, encoding, authInfo,
                bucketName, list, corsHeaders, log, callback);
        });
    });
    return undefined;
}

module.exports = {
    processMasterVersions,
    listLifecycleObjects,
};
