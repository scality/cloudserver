import xmlService from 'xml';

import services from '../services';

import querystring from 'querystring';

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


/*
   Construct JSON in proper format to be converted to XML
   to be returned to client
*/
function _constructJSON(json, xmlParams, listingParams) {
    let listBucketResultArray = [
			{ _attr: { xmlns: 'http://s3.amazonaws.com/doc/2006-03-01/' } },
			{ Name: xmlParams.bucketName },
			{ Prefix: listingParams.prefix },
			{ Marker: listingParams.marker },
			{ MaxKeys: listingParams.maxKeys },
			{ Delimiter: listingParams.delimiter },
			{ IsTruncated: json.IsTruncated },
    ];
    if (json.NextMarker) {
        listBucketResultArray.push({ NextMarker: json.NextMarker });
    }

    const commonPrefixes = json.CommonPrefixes.map(item => ({ Prefix: item }));

    if (commonPrefixes.length > 0) {
        const commonPrefixesObject = { CommonPrefixes: commonPrefixes };
        listBucketResultArray.push(commonPrefixesObject);
    }

    const contents = json.Contents.map(item => {
        const objectKey = xmlParams.encoding === 'url' ?
            querystring.escape(item.key) : item.key;
        return {
            Contents: [
                { Key: objectKey },
				{ LastModified: item.value.LastModified },
				{ ETag: item.value.ETag },
				{ Size: item.value.Size },
				{ Owner: [
                    { ID: item.value.Owner.ID },
                    { DisplayName: item.value.Owner.DisplayName }],
                },
				{ StorageClass: item.value.StorageClass },
            ],
        };
    });

    if (contents.length > 0) {
        listBucketResultArray = listBucketResultArray.concat(contents);
    }

    const constructedJSON = { ListBucketResult: listBucketResultArray };

    return constructedJSON;
}

function _convertToXml(json, xmlParams, listingParams) {
    const constructedJSON = _constructJSON(json, xmlParams, listingParams);
    const xml = xmlService(constructedJSON,
        { declaration: { standalone: 'yes', encoding: 'UTF-8' } });
    return xml;
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
    const maxKeys = params['max-keys'] ?
        Number.parseInt(params['max-keys'], 10) : 1000;
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: 'bucketGet',
        log,
    };
    const listParams = {
        maxKeys,
        delimiter: params.delimiter,
        marker: params.marker,
        prefix: params.prefix,
    };
    const xmlParams = {
        bucketName,
        encoding,
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
            const xml = _convertToXml(list, xmlParams, listParams);
            return callback(null, xml);
        });
    });
}
