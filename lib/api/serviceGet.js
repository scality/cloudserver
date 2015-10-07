'use strict';

const utils = require('../utils.js');
const services = require('../services.js');
const async = require('async');
const xml = require('xml');


/**
 * GET Service - Get list of buckets owned by user
 * @param  {string} accessKey - user's access key
 * @param {object} metastore - metastore with buckets containing objects and their metadata
 * @param {object} request - normalized request object
 * @return {function} callback with error, object data result and responseMetaHeaders as arguments
 */

let serviceGet = function(accessKey, metastore, callback){
	services.getService(accessKey, metastore, function(err, result) {
		return callback(null, 'test')
	});

	/*return vaultGetResource(vaultRequest, function(err, result){
		if(err){
			return callback(err);
		}
		let xml = _convertToXml(result, request, accessKey);
		return callback(null, xml);
	});*/
}

/*
	Format of xml response:

	<?xml version="1.0" encoding="UTF-8"?>
	<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01">
	  <Owner>
	    <ID>bcaf1ffd86f461ca5fb16fd081034f</ID>
	    <DisplayName>webfile</DisplayName>
	  </Owner>
	  <Buckets>
	    <Bucket>
	      <Name>quotes</Name>
	      <CreationDate>2006-02-03T16:45:09.000Z</CreationDate>
	    </Bucket>
	    <Bucket>
	      <Name>samples</Name>
	      <CreationDate>2006-02-03T16:41:58.000Z</CreationDate>
	    </Bucket>
	  </Buckets>
	</ListAllMyBucketsResult>*/


/*let _constructJSON = function(jsonFromVault, request, accessKey){
	let date = new Date(request.lowerCaseHeaders['date']);
	let month = (date.getMonth() + 1).toString();
	if(month.length === 1){
		month = `0${month}`;
	}

	let dateString = `${date.getFullYear()}-${month}-${date.getDate()}`;
	let hostname = request.lowerCaseHeaders.host.split(":")[0];

	let buckets = [];

	for(let i=0; i< jsonFromVault.bucketList.length; i++){
		let bucket = {
			"Bucket": [{"Name": jsonFromVault.bucketList[i].name},
			{"CreationDate": jsonFromVault.bucketList[i].creationDate}]
		}
		buckets.push(bucket);
	};


	let json = {
		"ListAllMyBucketsResult": [
			{_attr: {
				"xmlns": `http:\/\/${hostname}/doc/${dateString}`
			}},
			{"Owner":
				[{"ID": accessKey}, {"DisplayName": jsonFromVault.ownerDisplayName}]
			},
			{"Buckets":
				buckets
			}
		]
	};

	return json;
}

let _convertToXml = function(jsonFromVault, request, accessKey){
	let constructedJSON = _constructJSON(jsonFromVault, request, accessKey);
	let xml = xmlService(constructedJSON, { declaration: { standalone: 'yes', encoding: 'UTF-8' }});

	return xml;

};*/

module.exports = serviceGet;
