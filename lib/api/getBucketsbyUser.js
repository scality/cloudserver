var vaultGetResource = require("./services.js").vaultGetResource;
var xmlService = require('xml');


var _constructJSON = function(jsonFromVault){

	//construct JSON to send to xml

/*	<?xml version="1.0" encoding="UTF-8"?>
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

	

	return json;
}

var _convertToXml = function(jsonFromVault){

	var constructedJSON = _constructJSON(json);

	var xml = xmlService(jsonFromVault, true);

	return xml;

};

var getBucketsbyUser = function(accessKey, response, callback){

	var vaultRequest = {accessKey: accessKey, resource: "userBucketList"};

	vaultGetResource(vaultRequest, function(err, result){
		if(err){
			return callback(err);
		}
		//turn result into xml and add to response object
		console.log("here")

		var xml = _convertToXml(result);

		return callback(null, xml);
	});
};

module.exports = getBucketsbyUser;