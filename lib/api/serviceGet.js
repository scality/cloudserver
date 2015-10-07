'use strict';

const utils = require('../utils.js');
const services = require('../services.js');
const async = require('async');
const xmlService = require('xml');


/**
 * GET Service - Get list of buckets owned by user
 * @param  {string} accessKey - user's access key
 * @param {object} metastore - metastore with buckets containing objects and their metadata
 * @param {object} request - normalized request object
 * @return {function} callback with error, object data result and responseMetaHeaders as arguments
 */

let serviceGet = function(accessKey, metastore, request, callback) {
	services.getService(accessKey, metastore, request, function(err, result) {
		if(err){
			return callback(err);
		}
		let xml = _convertToXml(result, request, accessKey);
		console.log(xml);
		return callback(null, xml);
	});
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


let _constructJSON = function(userBuckets, request, accessKey){
	let date = new Date();
	let month = (date.getMonth() + 1).toString();
	if(month.length === 1){
		month = `0${month}`;
	}

	let dateString = `${date.getFullYear()}-${month}-${date.getDate()}`;
	let hostname = request.lowerCaseHeaders.host.split(":")[0];

	let buckets = [];

	for(let i=0, ii = userBuckets.length; i< ii; i++){
		let bucket = {
			"Bucket": [
				{
					"Name": userBuckets[i].name
				},
				{
					"CreationDate": userBuckets[i].creationDate
				}
			]
		}
		buckets.push(bucket);
	};


	let json = {
		"ListAllMyBucketsResult": [
			{
				_attr: {
					"xmlns": `http:\/\/${hostname}/doc/${dateString}`
			}
		},
		{
			"Owner":[
				{
					"ID": accessKey
				},
				{
					"DisplayName": accessKey
				}
			]
		},
		{
			"Buckets": buckets
		}
	]};

	return json;
}

let _convertToXml = function(data, request, accessKey){
	let constructedJSON = _constructJSON(data, request, accessKey);
	let xml = xmlService(constructedJSON, { declaration: { standalone: 'yes', encoding: 'UTF-8' }});

	return xml;

};

module.exports = serviceGet;
