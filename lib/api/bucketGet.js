import utils from '../utils.js';
import services from '../services.js';
import async from 'async';
import xmlService from'xml';

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

function _constructJSON(json, xmlParams) {
    const date = new Date(xmlParams.date);
    let month = (date.getMonth() + 1).toString();
    if (month.length === 1) {
        month = `0${month}`;
    }
    const dateString = `${date.getFullYear()}-${month}-${date.getDate()}`;
    const hostname = xmlParams.host;
    const listBucketResultArray = [
			{_attr: {
    "xmlns": `http:\/\/${hostname}/doc/${dateString}`
			}},
			{"Name": xmlParams.bucketname},
			{"Prefix": json.Prefix},
			{"Marker": json.Marker},
			{"MaxKeys": json.MaxKeys},
			{"Delimiter": json.Delimiter},
			{"IsTruncated": json.IsTruncated}
    ];
    if (json.NextMarker) {
        listBucketResultArray.push({"NextMarker": json.NextMarker});
    }


    const commonPrefixes = [];

    for (let i = 0; i < json.CommonPrefixes.length; i += 1) {
        const prefixItem = {
            "Prefix": json.CommonPrefixes[i]
        };
        commonPrefixes.push(prefixItem);
    }

    if (commonPrefixes.length > 0) {
        const commonPrefixesObject = {"CommonPrefixes": commonPrefixes};
        listBucketResultArray.push(commonPrefixesObject);
    }

    const contents = [];

    for (let i = 0; i < json.Contents.length; i += 1) {
        const contentItem = {
            "Contents": [
                {"Key": json.Contents[i].Key},
				{"LastModified": json.Contents[i].LastModified.toISOString()},
				{"ETag": json.Contents[i].ETag},
				{"Size": json.Contents[i].Size},
				{"Owner": [{"ID": json.Contents[i].Owner.ID},
                    {"DisplayName": json.Contents[i].Owner.DisplayName}
                ]},
				{"StorageClass": json.Contents[i].StorageClass}
            ]
        };
        contents.push(contentItem);
    }


    if (contents.length > 0) {
        Array.prototype.push.apply(listBucketResultArray, contents);
    }

    const constructedJSON = {
        "ListBucketResult": listBucketResultArray
    };

    return constructedJSON;
}

function _convertToXml(json, xmlParams) {
    const constructedJSON = _constructJSON(json, xmlParams);
    const xml = xmlService(constructedJSON,
        { declaration: { standalone: 'yes', encoding: 'UTF-8' }});
    return xml;
}

/**
 * bucketGet - Return list of objects in bucket
 * @param  {string} accessKey - user's accessKey
 * @param {object} metastore - metadata store
 * @param  {object} request - http request object
 * @param  {function} callback - callback to respond to http request
 *  with either error code or xml response body
 */

export default function bucketGet(accessKey, metastore, request, callback) {
    const params = request.query;
    const bucketname = utils.getResourceNames(request).bucket;
    const host = utils.getResourceNames(request).host;
    const bucketUID = utils.getResourceUID(request.namespace, bucketname);
    const metadataValParams = {
        accessKey,
        bucketUID,
        metastore,
        requestType: 'bucketGet'
    };
    const listingParams = {
        delimiter: params.delimiter,
        marker: params.marker,
        maxKeys: params.maxKeys,
        prefix: params.prefix
    };
    const xmlParams = {
        bucketname,
        host,
        date: request.lowerCaseHeaders.date,
    };

    async.waterfall([
        function waterfall1(next) {
            services.metadataValidateAuthorization(metadataValParams, next);
        },
        function watterfall2(bucket, extraArgumentFromPreviousFunction, next) {
            services.getObjectListing(bucket, listingParams, next);
        }
    ], function waterfallFinal(err, list) {
        if (err) {
            return callback(err, null);
        }
        const xml = _convertToXml(list, xmlParams);
        return callback(null, xml);
    });
}
