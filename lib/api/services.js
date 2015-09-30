"use strict";

var bucketLists = require("../testdata/vault.json").bucketLists;
let services = {};


 services.getBucketList = function(accessKey, callback){
	var result = bucketLists[accessKey];
	if(!result){
		return callback("No user data found");
	}
	return callback(null, result);
};

services.bucketExists = function(bucketname, cb) {
	for(let key in bucketLists){
		var userBuckets = bucketLists[key].bucketList;
		for(let i=0; i < userBuckets.length; i++){
			if(userBuckets[i].name === bucketname){
				return cb(null, true);
			}
		}
	}
	return cb(null, false);
};


services.objectExists = function(bucketname, objectKey, cb) {
	for(let key in bucketLists){
		var userBuckets = bucketLists[key].bucketList;
		for(let i=0; i < userBuckets.length; i++){
			if(userBuckets[i].name === bucketname){
				if(userBuckets[i].objects.indexOf(objectKey) > -1){
					return cb(null, true)
				}
			}
		}
	}
	return cb(null, false);
};

services.createBucket = function(bucketname, accessKey, region, callback) {
	_bucketExists(bucketname, function(exists) {
		if(exists) {
			return callback('Bucket already exists', null);
		}
		var userBuckets = bucketLists[accessKey].bucketList;
		var creationDate = new Date();
		userBuckets.push({
			name: bucketname,
			creationDate: creationDate.toISOString()
		});
		return callback(null,'Bucket created')
	});
};

services.isUserAuthorized = function(accessKey, bucketname, objectKey, isUserAuthorizedCallback) {
	var userInfo = bucketLists[accessKey];
	if(!userInfo){
		return isUserAuthorizedCallback(null, false);
	};
	var userBuckets = userInfo["bucketList"];

	for(let i=0; i<userBuckets.length; i++){
		if(userBuckets[i].name === bucketname){
			if(objectKey){
				if(userBuckets[i].objects.indexOf(objectKey) > -1){
					return isUserAuthorizedCallback(null, true);
				}
			}
			return isUserAuthorizedCallback(null, true);
		}
	}
	return isUserAuthorizedCallback(null, false);
};

// Generalized function to check whether a bucket exists and whether the given user has access.  
// For this implementation, authorization for all types of requests are collapsed into general authorization.
// The vault endpoints will have to deal with providing appropriate authorization checks for get versus put, etc.


services.bucketExistsandUserAuthorized = function(accessKey, bucketname, vaultGetResourceCallback) {

	_bucketExists(bucketname, function(exists) {
		if(!exists){
			//If bucket does not exist return status code 404
			return vaultGetResourceCallback(404)
		}
		//If bucket exists, check if this user as access to it
		_isUserAuthorized(accessKey, bucketname, function(is_authorized) {

			if(is_authorized){
				return vaultGetResourceCallback(200);
			}
			return vaultGetResourceCallback(403);
		});
	});
};

/**
 * Checks whether resource exists and the user is authorized
 * @param {object} [request] [custom built request object containing resource name, type, access key etc.]
 * @param {function} [cb] [callback containing result for the next task]
 */
services.metadataValidate = function(request, cb) {
	let self = this;
	async.parallel({
		bucketExists: function(callback){
	   	self.bucketExists(request.bucketname, callback);
	 	},
	  userAuthorized: function(callback){
	  	self.isUserAuthorized(request.accessKey, request.bucketname, request.objectKey, callback);
	 	},
	 	objectExists: function(callback){
	 		self.objectExists(request.bucketname, request.objectkey, callback)
	 	}
	 },
	  function(err, results){
	  	if(err){
	  		return cb(err);
	  	}
	  	if(results.bucketExists === false){
	  		return cb('Bucket does not exist');
	  	}
	  	if(results.userAuthorized === false){
	  		return cb('User does not have authorization');
	  	}
	  	if(results.objectExists === true){
	  		return cb(null, 'object exists');
	  	}
	  	return cb(null, 'success');
	  })
};



/**
 * Stores resource and responds back with location and storage type
 * @param {object} [request] [custom built request object containing resource name, resource body, type, access key etc.]
 * @param {function} [cb] [callback containing result for the next task]
 */
services.dataStore = function(objectExistsRes, ds, request, bucket, cb) {
	ds[bucket].PUTObject(request.key, request.value, function(location) {
		if(location) {
			return cb(null, objectExists, location);
		}
		return cb(null, objectExists, null);
	});
}

/**
 * Stores resource location, custom headers, version etc.
 * @param {object} [request] [custom built request object containing resource details.]
 * @param {function} [cb] [callback containing result for the next task]
 */
services.metadataStore = function(objectExistsRes, location, metastore, request, cb) {
	let isFreshObject = true;
	if(objectExistsRes === 'object exists'){
		isFreshObject = false;
	}

	let key = request.objectkey;

	metastore.key = {};

	//CONTINUE HERE

	//
	//aggregate metadata
	
	


	//update vault.json with new metadata or store in memory some other way
	cb();
}

module.exports = services;


/*module.exports = {

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

		if(vaultRequest.resource === 'createBucket') {
			return _createBucket(vaultRequest.bucketname, vaultGetResourceCallback);
		}
		
		if(vaultRequest.resource === "bucketHead"){
			return _bucketExistsandUserAuthorized(vaultRequest.accessKey, vaultRequest.bucketname, vaultGetResourceCallback);
		}

		if(vaultRequest.resource === "objectPut"){
			return _bucketExistsandUserAuthorized(vaultRequest.accessKey, vaultRequest.bucketname, vaultGetResourceCallback);
		}

		//Add other vault request types


		//Add other vault request types

		return vaultGetResourceCallback("Invalid resource request");
	}

};*/
