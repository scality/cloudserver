var bucketLists = require("../testdata/vault.json").bucketLists;


var _getBucketList = function(accessKey, callback){
	var result = bucketLists[accessKey];
	if(!result){
		return callback("No user data found");
	}
	return callback(null, result);
};

var _isDuplicateBucket = function(bucketname, callback) {
	var i, ii, x, userBuckets;
	for(x in bucketLists) {
		userBuckets = bucketLists[x].bucketList;
		userBuckets.forEach(function(b) {
			if(b.name === bucketname) {
				return callback('Bucket already exists', false);
			}
		});
	}
	return callback(null, true);
}


var _bucketHead = function(accessKey, bucket, callback) {

	return callback(2000);
};


module.exports = {

	vaultGetResource: function(vaultRequest, callback){
		if(!vaultRequest){
			return callback("No request made");
		}

		if(!vaultRequest.resource){
			return callback("Type of request not specified")
		}

		if(!vaultRequest.accessKey){
			return callback("No user information provided")
		}

		if(vaultRequest.resource === "userBucketList"){
			return _getBucketList(vaultRequest.accessKey, callback);
		}

		if(vaultRequest.resource === 'isDuplicateBucket') {
			return _isDuplicateBucket(vaultRequest.bucketname, callback);
		}
		if(vaultRequest.resource === "bucketHead"){
			return _bucketHead(vaultRequest.accessKey, vaultRequest.bucket, callback);
		}

		//Add other vault request types


		//Add other vault request types

		return callback("Invalid resource request");
	}

};
