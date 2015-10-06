'use strict';

const utils = require('../utils.js');
const services = require('../services.js');
const async = require('async');
const xmlService = require('xml');


//Sample XML response:
/*<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
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

let _constructJSON = function(json, xmlParams){
	let date = new Date(xmlParams.date);
	let month = (date.getMonth() + 1).toString();
	if(month.length === 1){
		month = `0${month}`;
	}

	let dateString = `${date.getFullYear()}-${month}-${date.getDate()}`;
	let hostname = xmlParams.host;

	let contents = [];

	for(let i=0; i< json.Contents.length; i+=1){
		let contentItem = {
			"Bucket": [{"Name": jsonFromVault.bucketList[i].name},
			{"CreationDate": jsonFromVault.bucketList[i].creationDate}]
		}
		contents.push(contentItem);
	};

	let commonPrefixes = [];

	for(let i=0; i< json.CommonPrefixes.length; i+=1) {
		let prefixItem = {
			"Prefix": json.CommonPrefixes[i]
		};
		commonPrefixes.push(prefixItem);
	}


	//Let's see if this will cause the xml to ignore the whole object in the array.
	let contentsObject = null;


	if(contents.length > 0){
		contentsObject = {"Contents": contents};
	}

	if(json.CommonPrefixes && json.CommonPrefixes.length > 0){
		commonPrefixesObject = {"CommonPrefixes:" commonPrefixes};
	}

	let listBucketResultArray = [
			{_attr: {
				"xmlns": `http:\/\/${hostname}/doc/${dateString}`
			}},
			{"Name": xmlParams.bucketname},
			{"Prefix": json.Prefix},
			{"Marker": json.Marker},
			{"MaxKeys": json.MaxKeys},
			{"Delimiter": json.Delimiter},
			{"IsTruncated": json.IsTruncated},
			contentsObject,
			commonPrefixesObject,
		]


	if(json.NextMarker) {
		listBucketResultArray.push(json.NextMarker);
	}

	if(commonPrefixes.length >0){
		//consider push
		listBucketResultArray.concat(commonPrefixes);
	}


	let constructedJSON = {
		"ListBucketResult": listBucketResultArray;
	};

	return constructedJSON;
}

let _convertToXml = function(json, xmlParams){
	let constructedJSON = _constructJSON(json, xmlParams);
	let xml = xmlService(constructedJSON, { declaration: { standalone: 'yes', encoding: 'UTF-8' }});

	return xml;

};

/**
 * bucketGet - Return list of objects in bucket
 * @param  {string} accessKey - user's accessKey
 * @param {object} metastore - metadata store
 * @param  {object} request - http request object
 * @param  {function} callback - callback to respond to http request with either error code or success
 */

let bucketGet = function(accessKey, metastore, request, callback) {
	let params = request.query;
	console.log("params", params)
	let bucketname = utils.getResourceNames(request).bucket;
	let bucketUID = utils.getResourceUID(request.namespace, bucketname);
	let metadataValParams = {accessKey: accessKey, bucketUID: bucketUID, metastore: metastore};
	let listingParams = {
		delimiter: params.delimiter, 
		marker: params.marker, 
		maxKeys: params.maxKeys, 
		prefix: params.prefix
		};
	let xmlParams = {bucketname: bucketname, date: request.lowerCaseHeaders.date, host: request.lowerCaseHeaders.host.split(":")[0]}



	async.waterfall([
	      function(next){
	        services.metadataValidateAuthorization(metadataValParams, next)
	      },
	       //CONTINUE HERE (call bucket_mem.js listing function with listingParams and convert response to xml)
	      function(bucket, extraArgumentFromPreviousFunction, next){
	        services.getObjectListing(bucket, listingParams, next);
	      }
	  ], function (err, list) {
	  	if(err) {
	  		return callback(err, null);
	  	}
	  	console.log("list", list);
			let xml = _convertToXml(list, xmlParams);
			return callback(null, xml);
	  });
	};


module.exports = bucketGet;
