var vaultGetResource = require("./services.js").vaultGetResource;


var getBucketsbyUser = function(accessKey, response, callback){

	var vaultRequest = {accessKey: accessKey, resource: "userBucketList"};

	vaultGetResource(vaultRequest, function(err, result){
		if(err){
			return callback(err);
		}
		//turn result into xml and add to response object
		return callback(null, response);
	});
};

module.exports = getBucketsbyUser;