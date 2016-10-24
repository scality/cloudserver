import querystring from 'querystring';
import constants from '../../constants';

import services from '../services';
import escapeForXML from '../utilities/escapeForXML';
import { errors, versioning } from 'arsenal';

const VSUtils = versioning.VersioningUtils;

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
    log.debug('processing request', { method: 'bucketGetVersions' });
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
            marker: params['key-marker'],
            versionIdMarker: params['version-id-marker'],
            prefix: params.prefix,
            versioningConfiguration: bucket.getVersioningConfiguration(),
            versions: true,
        };
        return services.getObjectListing(bucketName, listParams, log,
        (err, list) => {
            if (err) {
                log.debug('error processing request', { error: err });
                return callback(err);
            }
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
                const isLatest = latestVersions &&
                    versionId === latestVersions[key];
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
            return callback(null, xml.join(''));
        });
    });
    return undefined;
}
