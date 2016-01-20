import async from 'async';
import xmlService from'xml';

import services from '../services';
import utils from '../utils';

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

function _constructJSON(json, xmlParams, listingParams) {
    const date = new Date();
    let month = (date.getMonth() + 1).toString();
    if (month.length === 1) {
        month = `0${month}`;
    }
    const dateString = `${date.getFullYear()}-${month}-${date.getDate()}`;
    const hostname = xmlParams.host;
    let listBucketResultArray = [
			{_attr: { xmlns: `http://${hostname}/doc/${dateString}` }},
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


    const commonPrefixes = json.CommonPrefixes.map(item => {
        return { Prefix: item };
    });

    if (commonPrefixes.length > 0) {
        const commonPrefixesObject = { CommonPrefixes: commonPrefixes };
        listBucketResultArray.push(commonPrefixesObject);
    }

    const contents = json.Contents.map(item => {
        const objectKey = xmlParams.encoding === 'url' ?
            encodeURIComponent(item.key) : item.key;
        return {
            Contents: [
                { Key: objectKey },
				{ LastModified: item.value.LastModified },
				{ ETag: item.value.ETag },
				{ Size: item.value.Size },
				{ Owner: [
                    { ID: item.value.Owner.ID },
                    { DisplayName: item.value.Owner.DisplayName }, ]
                },
				{ StorageClass: item.value.StorageClass }

            ],
        };
    });

    if (contents.length > 0) {
        listBucketResultArray = listBucketResultArray.concat(contents);
    }

    const constructedJSON = {
        ListBucketResult: listBucketResultArray
    };

    return constructedJSON;
}

function _convertToXml(json, xmlParams, listingParams) {
    const constructedJSON = _constructJSON(json, xmlParams, listingParams);
    const xml = xmlService(constructedJSON,
        { declaration: { standalone: 'yes', encoding: 'UTF-8' }});
    return xml;
}

/**
 * bucketGet - Return list of objects in bucket
 * @param  {string} accessKey - user's accessKey
 * @param {object} metastore - metadata store
 * @param  {object} request - http request object
 * @param  {function} log - Werlogs request logger
 * @param  {function} callback - callback to respond to http request
 *  with either error code or xml response body
 */
export default function bucketGet(accessKey, metastore, request, log,
    callback) {
    log.debug('Processing the request in Bucket GET api');
    const params = request.query;
    const bucketName = utils.getResourceNames(request).bucket;
    log.debug(`Bucket Name: ${bucketName}`);
    const host = utils.getResourceNames(request).host;
    log.debug(`Host: ${host}`);
    const encoding = params['encoding-type'];
    const maxKeys = params['max-keys'] ?
        Number.parseInt(params['max-keys'], 10) : 1000;
    const metadataValParams = {
        accessKey,
        bucketName,
        metastore,
        requestType: 'bucketGet',
        log,
    };
    const listingParams = {
        maxKeys,
        delimiter: params.delimiter,
        marker: params.marker,
        prefix: params.prefix,
    };
    const xmlParams = {
        bucketName,
        host,
        encoding,
    };

    async.waterfall([
        function waterfall1(next) {
            services.metadataValidateAuthorization(metadataValParams, next);
        },
        function watterfall2(bucket, extraArgumentFromPreviousFunction, next) {
            services.getObjectListing(bucketName, listingParams, log, next);
        }
    ], function waterfallFinal(err, list) {
        if (err) {
            log.error(`Error while performing Bucket GET: ${err}`);
            return callback(err);
        }
        const xml = _convertToXml(list, xmlParams, listingParams);
        return callback(null, xml);
    });
}
