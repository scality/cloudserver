var bucketLists = {

	"accessKey1": {
		"ownerDisplayName": "BucketBaron",
		"bucketList": [
			{
				"name": "SuperBucket",
				"creationDate": "2006-02-03T16:45:09.000Z"
			}, 
			{
				"name": "SimplyBucket",
				"creationDate": "2009-02-03T16:42:06.000Z"
			}, 
			{	"name": "CharlieBucket",
				"creationDate": "2015-02-03T16:32:06.000Z"
			}]
	},

	"accessKey2": {
		"ownerDisplayName": "BucketBaller",
		"bucketList": [
			{
				"name": "SomeBucket",
				"creationDate": "2012-05-03T16:45:09.000Z"
			}, 
			{
				"name": "IceBucket",
				"creationDate": "2009-02-03T16:42:06.000Z"
			}]
	},
};


var _getBucketList = function(accessKey, callback){
	var result = bucketLists.accessKey;
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