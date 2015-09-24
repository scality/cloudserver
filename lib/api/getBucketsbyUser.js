var vaultGetResource = require("./services.js").vaultGetResource;
var xmlService = require('xml');


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


var _constructJSON = function(jsonFromVault, request, accessKey){
	var date = new Date(request.lowerCaseHeaders['date']);
	var month = (date.getMonth() + 1).toString();
	if(month.length === 1){
		month = `0${month}`;
	}

	var dateString = `${date.getFullYear()}-${month}-${date.getDate()}`;
	var hostname = request.lowerCaseHeaders.host.split(":")[0];

	var buckets = [];

	for(var i=0; i< jsonFromVault.bucketList.length; i++){
		var bucket = {
			"Bucket": [{"Name": jsonFromVault.bucketList[i].name},
			{"CreationDate": jsonFromVault.bucketList[i].creationDate}]
		}
		buckets.push(bucket);
	};


	var json = {
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

var _convertToXml = function(jsonFromVault, request, accessKey){
	var constructedJSON = _constructJSON(jsonFromVault, request, accessKey);
	var xml = xmlService(constructedJSON, { declaration: { standalone: 'yes', encoding: 'UTF-8' }});

	return xml;

};

/**
 * GET Service - Get list of buckets owned by user
 * @param  {object} request - http request object
 * @param  {object} response - http response object
 */

var getBucketsbyUser = function(accessKey, request, response, callback){

	var vaultRequest = {accessKey: accessKey, resource: "userBucketList"};

	vaultGetResource(vaultRequest, function(err, result){
		if(err){
			return callback(err);
		}
		var xml = _convertToXml(result, request, accessKey);
		return callback(null, xml);
	});
};

module.exports = getBucketsbyUser;