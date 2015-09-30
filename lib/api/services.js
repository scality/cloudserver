"use strict";

const bucketLists = require("../testdata/vault.json").bucketLists;
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
 *
 * 
 */

services.putDataStore = function(key, value, callback) {
	ds.key = value;
	//For in memory implementation
	let location = key;
	callback(null, location);
}

/**
 * Checks whether resource exists and the user is authorized
 * @param {object} [params] [custom built object containing resource name, type, access key etc.]
 * @param {function} [cb] [callback containing result for the next task]
 */
services.metadataValidateAuthorization = function(params, cb) {
	let self = this;	
	let store = params.metastore;
	let bucketUID = params.bucketUID;
	let objectUID = params.objectUID;

	if(store.bucketUID === undefined){
		return cb("Bucket does not exist");
	}

	let bucket = store.bucketUID;

	//Parse xml permissions
	//if not permitted, return cb("Action not permitted")
	//else cb(null, bucket, 'object exists')
	



};



/**
 * Stores resource and responds back with location and storage type
 * @param {object} [params] [custom built object containing resource name, resource body, type, access key etc.]
 * @param {function} [cb] [callback containing result for the next task]
 */
services.dataStore = function(bucket, objectExistsRes, ds, params, cb) {
	//create unique here for key. take namespace, bucket name/object name and create unique hash --> key
	//hey data, take this.  no bucket.js involved.  
	//
	let key = params.objectUID;
	this.putDataStore(key, params.value, function(err, location){
		if(err){
			cb(err, null, null, null, null);
		}
		if(location) {
			return cb(null, bucket, objectExistsRes, location);
		}
		return cb(null, bucket, null, null);
	});

}

/**
 * Stores resource location, custom headers, version etc.
 * @param {object} [params] [custom built object containing resource details.]
 * @param {function} [cb] [callback containing result for the next task]
 */
services.metadataStore = function(bucket, objectExistsRes, location, metastore, params, cb) {
	let omVal;
	if(objectExistsRes !== 'object exists'){
		omVal = {};
		omVal['Date'] = new Date();
	} else {
		omVal = bucket.keymap.objectUID;
	}



	//metastore.bucketname.key
	//metadata is just key/value 
	//each bucket should be different table in metadata.
	//metastore.bucket.key
	omVal['Content-Length'] = params.headers['content-length'];
	//confirm date format
	omVal['Last-Modified'] = new Date();
	omVal['Content-MD5'] = //MD5
	//Need to complete values
	omVal['x-amz-server-side-encryption'] = 1;
	omVal['x-amz-server-version-id'] = 2;
	omVal['x-amz-delete-marker'] = 3;
	omVal['x-amz-storage-class'] = 4;
	omVal['x-amz-website-redirect-location'] = 5;
	omVal['x-amz-server-side-encryption-aws-kms-key-id'] = 6;
	omVal['x-amz-server-side-encryption-customer-algorithm'] = 7;

	for(let k in params.headers){
		if(i.substr(0, 11) === 'x-amz-meta-'){
			omVal.k = params.headers.k;
		}
	}

	omVal['Location'] = location;
	//CONTINUE HERE

	//
	//aggregate metadata
	
	// ds[bucket].PUTObject(request.objectKey, request.value, function(location) {
	// 	if(location) {
	// 		return cb(null, objectExists, location);
	// 	}
	// 	return cb(null, objectExists, null);
	// });


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
