"use strict";

const async = require('async');
const bucketLists = require("./testdata/vault.json").bucketLists;
const Bucket = require('./bucket_mem.js');

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
 * Called by services.dataStore to actually put the key and value in the datastore
 * @param {object} ds - in memory datastore
 * @param {string} key - object key
 * @param {string} value - object value (request.post)
 * @param {function} callback - callback to services.dataStore
 */

services.putDataStore = function(ds, key, value, callback) {
	//In memory implementation
	ds[key] = value;
	let location = key;
	callback(null, location);
}

/**
 * Checks whether resource exists and the user is authorized
 * @param {object} params - custom built object containing resource name, type, access key etc.
 * @param {function} cb - callback containing error, bucket and object references for the next task
 */
services.metadataValidateAuthorization = function(params, cb) {
	let store = params.metastore;
	let bucketUID = params.bucketUID;
	let objectUID = params.objectUID;

	if(store[bucketUID] === undefined){
		return cb("Bucket does not exist -- 404", null, null);
	}

	let bucket = store[bucketUID];

	//For now, user is authorized if they are bucket owner.  In future implementation
	//will parse xml permissions more particularly.
	if(bucket.owner !== params.accessKey){
		return cb("Action not permitted -- 403", null, null);
	}

	if(objectUID === undefined) {
		return cb(null, bucket, null);
	}

	bucket.GETObject(objectUID, function(objectMetadataNotFound, objectMetadata, objectUID) {
		if(objectMetadataNotFound) {
			return cb(null, bucket, null);
		}
		return cb(null, bucket, objectMetadata);
	});
};



/**
 * Stores object and responds back with location and storage type
 * @param {object} params - custom built object containing resource name, resource body, type, access key etc.
 * @param {function} cb - callback containing result for the next task
 */
services.dataStore = function(bucket, objectMetadata, ds, params, cb) {
	if(params.headers['content-md5']){
		if(params.headers['content-md5'] !== params.contentMD5){
			cb("Content-MD5 is invalid", null, null, null);
		}
	}


	let key = params.objectUID;
	this.putDataStore(ds, key, params.value, function(err, newLocation){
		if(err){
			cb(err, null, null, null, null);
		}
		if(newLocation) {
			return cb(null, bucket, objectMetadata, newLocation);
		}
		return cb(null, bucket, null, null);
	});

}

/**
 * Stores object location, custom headers, version etc.
 * @param {object} params - custom built object containing resource details.
 * @param {function} cb - callback containing result for the next task
 */

services.metadataStoreObject = function(bucket, objectMetadata, newLocation, metastore, params, cb) {
	let omVal;
	if(objectMetadata){
		//Save then current location to have prior location?
		omVal = objectMetadata;
	} else {
		omVal = {};
		omVal['Date'] = new Date();
		omVal['objectUID'] = params.objectUID;

		//AWS docs state that the user that creates a resource is the owner.  
		//Assumed here that original creator remains owner even if later Puts to object.
		//If an IAM user uploads a resource, the owner should be the parent. 
		//TODO: Need to update this to handle IAM users.
		//http://docs.aws.amazon.com/AmazonS3/latest/dev/access-control-overview.html
		omVal['owner-display-name'] = params.accessKey;
		//TODO: This should be object creator's canonical ID.  
		omVal['owner-id'] = 'canonicalIDtoCome';
	}

	omVal['content-length'] = params.headers['content-length'];
	//confirm date format
	omVal['last-modified'] = new Date();
	omVal['content-md5'] = params.contentMD5;
	//Need to complete values
	omVal['x-amz-server-side-encryption'] = "";
	omVal['x-amz-server-version-id'] = "";
	omVal['x-amz-delete-marker'] = "";
	//Hard-coded storage class as Standard.  Could have config option.
	omVal['x-amz-storage-class'] = "Standard";
	omVal['x-amz-website-redirect-location'] = "";
	omVal['x-amz-server-side-encryption-aws-kms-key-id'] = "";
	omVal['x-amz-server-side-encryption-customer-algorithm'] = "";
	omVal['location'] = newLocation;
  // simple/no version. will expand once object versioning is introduced
  omVal['x-amz-version-id'] = null;

	//Store user provided metadata.  TODO: limit size.
	for(let k in params['metaHeaders']){
		omVal[k] = params['metaHeaders'][k];
	}

	bucket.PUTObject(params.objectKey, omVal, function (err) {
		if(err) {
			return cb(err, null);
		}
		return cb(null, params.contentMD5);
	});

};


/**
 * Checks whether user is authorized to create a bucket and whether the bucket already exists.
 * If user is authorized and bucket does not already exist, bucket is created and saved in metastore
 * along with metadata provided with request.
 * @param {string} accessKey - user's access key
 * @param {string} bucketUID - unique identifier for bucket
 * @param {object} headers - request headers
 * @param {string} locationConstraint - locationConstraint provided in request body xml (if provided)
 * @param {object} metastore - global metastore
 * @param {function} callback - callback to bucketPut
 */

services.createBucket = function(accessKey, bucketUID, headers, locationConstraint, metastore, callback) {

	async.waterfall([
        function(next){
        	//TODO Check user policies to see if user is authorized to create a bucket
	        if(metastore[bucketUID] !== undefined){
	          next("Bucket already exists", null);
	        }
	        next();
        },
        function(next){
        	let bucket = new Bucket();
        	bucket.owner = accessKey;

        	if(locationConstraint !== undefined) {
        		bucket.locationConstraint = locationConstraint;
        	}

        	if(headers['x-amz-acl']){
        		bucket.acl = headers['x-amz-acl'];
        	}

        	if(headers['x-amz-grant-read']){
        		bucket.acl['x-amz-grant-read'] = headers['x-amz-grant-read'];
        	}

        	if(headers['x-amz-grant-write']){
        		bucket.acl['x-amz-grant-write'] = headers['x-amz-grant-write'];
        	}

        	if(headers['x-amz-grant-read-acp']){
        		bucket.acl['x-amz-grant-read-acp'] = headers['x-amz-grant-read-acp'];
        	}

        	if(headers['x-amz-grant-write-acp']){
        		bucket.acl['x-amz-grant-write-acp'] = headers['x-amz-grant-write-acp'];
        	}

        	if(headers['x-amz-grant-full-control']){
        		bucket.acl['x-amz-grant-full-control'] = headers['x-amz-grant-full-control'];
        	}

        	metastore[bucketUID] = bucket;
        	next(null, "Bucket created");
        }
    ], function (err, result) {
      callback(err, result);
    });
};

/**
 * Gets object from datastore
 * @param {object} bucket - bucket in which objectMetadata is stored
 * @param {object} objectMetadata - object's metadata
 * @param {object} responseMetaHeaders - contains user meta headers to be passed to response
 * @param {object} ds - datastore
 * @param {function} cb - callback from async.waterfall in objectGet
 */

services.getFromDatastore = function(bucket, objectMetadata, responseMetaHeaders, ds, cb) {
	//TODO: Handle range requests
	let location = objectMetadata['location'];
	let result = ds[location];
	if(!result) {
		return cb("Object not found in datastore", null, null);
	}
	cb(null, result, responseMetaHeaders);
};

/**
 * Deletes objects from a bucket
 * @param {object} bucket - bucket in which objectMetadata is stored
 * @param {object} objectMetadata - object's metadata
 * @param {object} responseMetaHeaders - contains user meta headers to be passed to response
 * @param {string} object unique identifier
 * @param {function} cb - callback from async.waterfall in objectGet
 */
services.deleteObjectFromBucket = function(bucket, objectMetadata, responseMetaHeaders, objectUID, cb) {

  if(objectMetadata['x-amz-delete-marker']) {
    responseMetaHeaders['x-amz-delete-marker'] = true;
  } else if(objectMetadata['x-amz-version-id'] !== undefined) {
    objectMetadata['x-amz-delete-marker'] = true;

    bucket.DELETEObject(objectUID, function (err) {
      if(err) {
        return cb(err, null, responseMetaHeaders);
      }
      return cb(null, 'Object deleted permanently', responseMetaHeaders);
    });
  } else {
    objectMetadata['x-amz-delete-marker'] = true;
    return cb(null, 'Object marked as deleted', responseMetaHeaders);
  }
}

/**
 * Delete bucket from namespace
 * @param {object} bucket - bucket in which objectMetadata is stored
 * @param {object} objectMetadata - object's metadata
 * @param {object} responseMetaHeaders - contains user meta headers to be passed to response
 * @param {string} object unique identifier
 * @param {function} cb - callback from async.waterfall in objectGet
 */
services.deleteBucket = function(bucket, responseMetaHeaders, metastore, bucketUID, cb) {
  var bucketMetadata = metastore[bucketUID];

  if(Object.keys(bucket.keyMap).length > 0) {
    return cb('Bucket is not empty', null, responseMetaHeaders);
  }

  delete metastore[bucketUID];
  bucket.DELETEBucket(function (err) {
    if(err) {
      return cb(err, null, responseMetaHeaders);
    }
    return cb(null, 'Bucket deleted permanently', responseMetaHeaders);
  });
}


services.bucketMetadataChecks = function(bucket, metadataCheckParams, cb) {
  // todo check request headers and add appropriate response headers
  return cb(null, bucket, metadataCheckParams);
}
/**
 * Checks whether request headers included 'if-modified-since', 'if-unmodified-since', 'if-match' or 'if-none-match'
 * headers.  If so, return appropriate response based on last-modified date of object or Etag.
 * Also pulls user's meta headers from metadata and passes them along to be added to response.
 * @param {object} bucket - bucket in which objectMetadata is stored
 * @param {object} objectMetadata - object's metadata
 * @param {object} metadataCheckParams - contains lowercased headers from request object
 * @param {function} cb - callback from async.waterfall in objectGet
 */

services.metadataChecks = function(bucket, objectMetadata, metadataCheckParams, cb) {

	if(!objectMetadata) {
		return cb('Object not found', null, null, null);
	}

	let headers = metadataCheckParams.headers;
	let lastModified = objectMetadata['last-modified'].getTime();
	let contentMD5 = objectMetadata['content-md5'];
	let ifModifiedSinceTime = headers['if-modified-since'];
	let ifUnmodifiedSinceTime = headers['if-unmodified-since'];
	let ifEtagMatch = headers['if-match'];
	let ifEtagNoneMatch = headers['if-none-match'];


	if(ifModifiedSinceTime) {
		ifModifiedSinceTime = new Date(ifModifiedSinceTime);
		ifModifiedSinceTime = ifModifiedSinceTime.getTime();
		if(isNaN(ifModifiedSinceTime)){
			return cb('Invalid modification date provided', null, null, null);
		}
		if(lastModified < ifModifiedSinceTime){
			return cb('Not modified -- 304', null, null, null);
		}

	}

	if(ifUnmodifiedSinceTime) {
		ifUnmodifiedSinceTime = new Date(ifUnmodifiedSinceTime);
		ifUnmodifiedSinceTime = ifUnmodifiedSinceTime.getTime();
		if(isNaN(ifUnmodifiedSinceTime)){
			return cb('Invalid modification date provided', null, null, null);
		}
		if(lastModified > ifUnmodifiedSinceTime){
			return cb('Precondition failed -- 412', null, null, null);
		}
	}

	if(ifEtagMatch) {
		if(ifEtagMatch !== contentMD5){
			return cb('Precondition failed -- 412', null, null, null)
		}
	}

	if(ifEtagNoneMatch) {
		if(ifEtagNoneMatch === contentMD5){
			return cb('Not modified -- 304', null, null, null)
		}
	}

	//Add user meta headers from objectMetadata

	let responseMetaHeaders = {};

	for(let k in objectMetadata){
    if(k.substr(0, 11) === 'x-amz-meta-'){
      responseMetaHeaders[k] = objectMetadata[k];
    }
  }

  responseMetaHeaders['Content-Length'] = objectMetadata['content-length'];
  responseMetaHeaders['Etag'] = objectMetadata['content-md5'];

  return cb(null, bucket, objectMetadata, responseMetaHeaders)


}

/**
 * Gets list of objects in bucket
 * @param {object} bucket - bucket in which objectMetadata is stored
 * @param {object} listingParams - params object passing on needed items from request object
 * @param {function} cb - callback to bucketGet.js
 * @returns {function} callback with either error or JSON response from metastore
 */


services.getObjectListing = function(bucket, listingParams, cb) {
	let delimiter = listingParams.delimiter;
	let marker = listingParams.marker;
	let maxKeys = Number(listingParams.maxKeys);
	let prefix = listingParams.prefix;

	bucket.GETBucketListObjects(prefix, marker, delimiter, maxKeys, function(err, listResponse) {
		if(err) {
			return cb(err, null);
		}
		return cb(null, listResponse);
	});


}

module.exports = services;
