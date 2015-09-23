var bucketLists = require("../testdata/vault.json").bucketLists;


var _getBucketList = function(accessKey, callback){
	var result = bucketLists[accessKey];
	if(!result){
		return callback("No user data found.");
	}
	return callback(null, result);
};



module.exports = {

	vaultGetResource: function(vaultRequest, callback){
		if(!vaultRequest){
			return callback("No request made.");
		}
		if(!vaultRequest.resource){
			return callback("Type of request not specified.")
		}
		if(!vaultRequest.accessKey){
			return callback("No user information provided.")
		}

		if(vaultRequest.resource === "userBucketList"){
			return _getBucketList(vaultRequest.accessKey, callback);
		}

		//Add other vault request types


		return callback("Invalid resource request.");

	}

};