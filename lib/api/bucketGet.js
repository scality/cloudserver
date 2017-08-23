const querystring = require('querystring');
const { errors, versioning, s3middleware, LivyClient } = require('arsenal');

const constants = require('../../constants');
const services = require('../services');
const { metadataValidateBucket } = require('../metadata/metadataUtils');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const escapeForXml = s3middleware.escapeForXml;
const { pushMetric } = require('../utapi/utilities');

const versionIdUtils = versioning.VersionID;

const config = require('../Config.js').config;
const werelogs = require('werelogs');
werelogs.configure({ level: config.log.logLevel,
    dump: config.log.dumpLevel });
const log = new werelogs.Logger('LivyClient');
const useHttps = !!config.livy.transport.https;
const livyClient = new LivyClient(config.livy.host,
    config.livy.port, log, useHttps);
let defaultSession = 0;
const setUpSessionCode = 'import com.scality.clueso._\n' +
    'import com.scality.clueso.query._\n' +
    'val config = com.scality.clueso.SparkUtils.' +
    'loadCluesoConfig("/apps/spark-modules/application.conf"); \n' +
    'SparkUtils.confSparkSession(spark,config); \n' +
    'val queryExecutor = MetadataQueryExecutor(spark, config); \n';
const sessionConfig = {
    kind: 'spark',
    numExecutors: 2,
    executorMemory: '512m',
    jars: ['/apps/spark-modules/clueso-1.0-SNAPSHOT-all.jar'],
    conf: { 'spark.hadoop.fs.s3a.impl':
        'org.apache.hadoop.fs.s3a.S3AFileSystem',
        'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
        // TODO: We need to figure out how to configure this directly in spark
        // or could use the restEndpoints from the config
        'spark.hadoop.fs.s3a.endpoint': 'lb',
        // TODO: For Zenko, we can send admin keys but for enterprise version,
        // s3 will not have access to keys.  So, this should be set
        // in spark config directly on deployment.
        'spark.hadoop.fs.s3a.access.key': 'accessKey1',
        'spark.hadoop.fs.s3a.secret.key': 'verySecretKey1',
        'spark.hadoop.fs.s3a.path.style.access': 'true',
        'spark.driver.extraJavaOptions':
            '-agentlib:jdwp=transport=dt_socket,server=y,' +
            'suspend=n,address=5005',
        'spark.sql.parquet.cacheMetadata': 'false',
        'spark.cores.max': '2',
        'spark.driver.port': '38600',
    },
};

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
        { tag: 'Marker', value: listParams.marker || '' },
        { tag: 'NextMarker', value: list.NextMarker },
        { tag: 'MaxKeys', value: listParams.maxKeys },
        { tag: 'Delimiter', value: listParams.delimiter },
        { tag: 'EncodingType', value: listParams.encoding },
        { tag: 'IsTruncated', value: isTruncated },
    ];

    const escapeXmlFn = listParams.encoding === 'url' ?
        querystring.escape : escapeForXml;
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

function handleStatement(sessionId, codeToExecute, corsHeaders, bucketName,
    listParams, log, callback) {
    log.info('about to postStatement with codeToExecute', { codeToExecute });
    return livyClient.postStatement(sessionId,
        codeToExecute, (err, res) => {
            console.log("code executed!!!", codeToExecute)
            console.log("err from livy on postStatement!!", err)
            if (err) {
                log.info('error from livy posting ' +
                'statement', { error: err.message });
                return callback(errors.InternalError
                    .customizeDescription('Error ' +
                    'performing search'),
                    null, corsHeaders);
            }
            if (!res || !Number.isInteger(res.id)) {
                log.error('posting statement did not result ' +
                'in valid statement id', { resFromLivy: res });
                return callback(errors.InternalError
                    .customizeDescription('Error ' +
                    'performing search'),
                    null, corsHeaders);
            }
            return livyClient.getStatement(sessionId, res.id, (err, res) => {
                if (err) {
                    console.log("err from livy on get statement!!", err)
                    log.info('error from livy getting ' +
                    'statement', { error: err.message });
                    return callback(errors.InternalError
                        .customizeDescription('Error ' +
                        'performing search'),
                        null, corsHeaders);
                }
                if (!res || !res.data || !res.status === 'ok') {
                    log.error('getting statement did not result ' +
                    'in valid result', { resFromLivy: res });
                    return callback(errors.InternalError
                        .customizeDescription('Error ' +
                        'performing search'),
                        null, corsHeaders);
                }
                console.log("res from livy on get Statement!!!", res);
                // const result = res.data['text/plain'];
                const list = { CommonPrefixes: [] };
                list.Contents = [
                    { key: res, value: {
                        LastModified: 'sometime',
                        ETag: 'myetag',
                        Owner: { ID: 'ownerId123', DisplayName: 'me' },
                        Size: '100',
                        StorageClass: 'standard',
                    },
                }];
                // TODO: (1) handle versioning,
                // (2) handle proper entry creation/parsing,
                // (3) handle nextMarkers -- nextMarker should be
                // the last key returned if the number of keys is more than the
                // max keys (since we request max plus 1)
                const xml = processMasterVersions(bucketName, listParams, list);
                return callback(null, xml, corsHeaders);
            });
        });
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
function bucketGet(authInfo, request, log, callback) {
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
    };
    const listParams = {
        listingType: 'DelimiterMaster',
        maxKeys: actualMaxKeys,
        delimiter: params.delimiter,
        marker: params.marker,
        prefix: params.prefix,
    };

    metadataValidateBucket(metadataValParams, log, (err, bucket) => {
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
                versionIdUtils.decode(params['version-id-marker']) : undefined;
        }
        // TODO: get rid of || params.search here!!
        console.log("fix this")
        if (params.search !== undefined || params.search === 'test') {
            log.info('performaing search listing', { search: params.search });
            // params.search should just be sql where clause
            // For metadata: message.userMd.`x-amz-meta-color`=\"blue\"
            // For tags: message.tags.`x-amz-meta-color`=\"blue\"
            // For any other attribute: message.value.`content-length`=5
            // TODO: Fix this!!! use first version or add parsing...
            // const whereClause = params.search;
            const whereClause = params.search === 'test' ? '' : params.search;
            // spark should return keys starting AFTER marker alphabetically
            // spark should return up to maxKeys
            const start = listParams.marker ? `Some("${listParams.marker}")` :
                'None';
            const searchCodeToExecute =
                'queryExecutor.executeAndPrint(MetadataQuery' +
                `("${bucketName}", "${whereClause}", ${start}, ` +
                // Add one to the keys requested so we can use the last key
                // as a next marker if needed
                // Might just need the last one???!!!
                `${listParams.maxKeys + 1}));\n`;
            // TODO: consider doing listing of sessions and finding
            // available session
            // if better to have more than one running.
            // Could also handle status of busy by creating new session
            // if it would help
            return livyClient.getSessionOrBatchState('session', defaultSession,
                (err, res) => {
                    if (err ||
                        (res && res.state !== 'idle' &&
                        res.state !== 'busy' &&
                        res.state !== 'starting')) {
                        log.info('livy session not found or not in state to ' +
                        'accept statement. creating',
                        { error: err, state: res });
                        return livyClient.postSession(sessionConfig,
                        (err, res) => {
                            if (err) {
                                log.info('error from livy creating session',
                                { error: err.message });
                                return callback(errors.InternalError
                                    .customizeDescription('Error ' +
                                    'performing search'),
                                    null, corsHeaders);
                            }
                            if (!res || !Number.isInteger(res.id)) {
                                log.error('posting session did not result ' +
                                'in valid session id', { resFromLivy: res });
                                return callback(errors.InternalError
                                    .customizeDescription('Error ' +
                                    'performing search'),
                                    null, corsHeaders);
                            }
                            defaultSession = res.id;
                            const codeToExecute = `${setUpSessionCode} ` +
                            `${searchCodeToExecute};`;
                            return handleStatement(res.id, codeToExecute,
                                corsHeaders, bucketName, listParams, log,
                                callback);
                        });
                    }
                    // no need to create session
                    return handleStatement(defaultSession, searchCodeToExecute,
                        corsHeaders, bucketName, listParams, log, callback);
                });
        }
        return services.getObjectListing(bucketName, listParams, log,
        (err, list) => {
            if (err) {
                log.debug('error processing request', { error: err });
                return callback(err, null, corsHeaders);
            }
            listParams.maxKeys = requestMaxKeys;
            listParams.encoding = encoding;
            let res;
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

module.exports = bucketGet;
