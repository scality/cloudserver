var utils = require('../utils.js');
var vaultGetResource = require("./services.js").vaultGetResource;


var bucketHead = function(accessKey, request, response, callback) {
	var bucket = utils.getBucketName(request);
	var vaultRequest = {accessKey: accessKey, resource: "bucketHead", bucket: bucket};
	return vaultGetResource(vaultRequest, function(responseCode){
		console.log("responseCode", responseCode)
		return callback(responseCode);
	});

};

module.exports = bucketHead;