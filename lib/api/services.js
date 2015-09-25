"use strict";

var bucketLists = require("../testdata/vault.json").bucketLists;


var _getBucketList = function(accessKey, callback){
	var result = bucketLists[accessKey];
	if(!result){
		return callback("No user data found");
	}
	return callback(null, result);
};

var _bucketExists = function(bucketname, callback) {
	for(let key in bucketLists){
		var userBuckets = bucketLists[key].bucketList;
		for(let i=0; i < userBuckets.length; i++){
			if(userBuckets[i].name === bucketname){
				return callback(true);
			}
		}
	}
	return callback(false);

};

var _isDuplicateBucket = function(bucketname, callback) {
	_bucketExists(bucketname, function(exists){
		if(exists){
			return callback('Bucket already exists', false);
		}
		return callback(null, true);
	});
};

var _isUserAuthorized = function(accessKey, bucketname, _isUserAuthorizedCallback) {
	var userInfo = bucketLists[accessKey];
	if(!userInfo){
		return _isUserAuthorizedCallback(false)
	};
	var userBuckets = userInfo["bucketList"];

	for(let i=0; i<userBuckets.length; i++){
		if(userBuckets[i].name === bucketname){
			return _isUserAuthorizedCallback(true);
		}
	}
	return _isUserAuthorizedCallback(false);
};


var _bucketHead = function(accessKey, bucketname, vaultGetResourceCallback) {
	console.log("bucketname in _bucketHead", bucketname)

	_bucketExists(bucketname, function(exists) {
		if(!exists){
			//If bucket does not exist return status code 404
			return vaultGetResourceCallback(404)
		}
		//If bucket exists, check if this user as access to it
		_isUserAuthorized(accessKey, bucketname, function(is_authorized) {
			console.log("in is user authorized callback", is_authorized)

			if(is_authorized){
				console.log("in if block")
				// console.log('cb in _isUserAuthorized', callback.toString())
				return vaultGetResourceCallback(200);
			}
			console.log("is not authorized in _buckethead")
			return vaultGetResourceCallback(403);
		});
	});
};


module.exports = {

	vaultGetResource: function(vaultRequest, vaultGetResourceCallback){
		if(!vaultRequest){
			return vaultGetResourceCallback("No request made");
		}

		if(!vaultRequest.resource){
			return vaultGetResourceCallback("Type of request not specified")
		}

		if(!vaultRequest.accessKey){
			return vaultGetResourceCallback("No user information provided")
		}

		if(vaultRequest.resource === "userBucketList"){
			return _getBucketList(vaultRequest.accessKey, vaultGetResourceCallback);
		}

		if(vaultRequest.resource === 'isDuplicateBucket') {
			return _isDuplicateBucket(vaultRequest.bucketname, vaultGetResourceCallback);
		}
		if(vaultRequest.resource === "bucketHead"){
			console.log("vaultRequest.bucketname", vaultRequest.bucketname)
			return _bucketHead(vaultRequest.accessKey, vaultRequest.bucketname, vaultGetResourceCallback);
		}

		//Add other vault request types


		//Add other vault request types

		return vaultGetResourceCallback("Invalid resource request");
	}

};
