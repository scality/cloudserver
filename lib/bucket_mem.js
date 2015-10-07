'use strict';

var utilities = require("./bucket_utilities.js");
var markerFilter = utilities.markerFilter;
var prefixFilter = utilities.prefixFilter;
var findNextMarker = utilities.findNextMarker;
var async = require("async");
var DEFAULT_MAX_KEYS = 1000;

var ListBucketResult = function() {
    this.Errors = [];
    this.IsTruncated = false;
    this.NextMarker = undefined;
    this.CommonPrefixes = [];
    this.Contents = [];
/*    Note:  this.MaxKeys will get incremented as keys are added so that when response is returned, this.MaxKeys will
    equal total keys in response (with each CommonPrefix counting as 1 key)*/
    this.MaxKeys = 0;
};

//Do we want to have the errors in the result or log somewhere else?
ListBucketResult.prototype.errorMessage = function(reason) {
    this.Errors.push(reason);
};


ListBucketResult.prototype.addContentsKey = function(key, keyMap) {

	var objectMetadata = keyMap[key];

  this.Contents.push({
  	"Key": key,
  	"LastModified": objectMetadata['last-modified'],
  	"ETag": objectMetadata['content-md5'],
  	"StorageClass": objectMetadata['x-amz-storage-class'],
  	"Owner": {"DisplayName": objectMetadata['owner-display-name'], "ID": objectMetadata['owner-id']},
  	"Size": objectMetadata['content-length']
	});
  this.MaxKeys++;
};

ListBucketResult.prototype.addCommonPrefix = function(prefix) {
  if(!this.hasCommonPrefix(prefix)) {
		this.CommonPrefixes.push(prefix);
		this.MaxKeys++;
  }
};

ListBucketResult.prototype.hasCommonPrefix = function(prefix) {
  return (this.CommonPrefixes.indexOf(prefix) !== -1);
};

var Bucket = function() {
    this.keyMap = {};
    this.acl = {};
    this.policy = {};
    this.owner = "";
    this.name = "";
    this.creationDate = new Date;
};


Bucket.prototype.PUTObject = function(key, value, callback) {
		//Production
		//http client call but separate file

		//In memory
    var _this = this;
    process.nextTick(function() {
			_this.keyMap[key] = value;
			if(callback) {
			  callback();
		  }
    });
};

Bucket.prototype.GETObject = function(key, callback) {
  var _this = this;
  process.nextTick(function() {
		var hasKey = _this.keyMap.hasOwnProperty(key);
		if(callback) {
		  callback(!hasKey, _this.keyMap[key], key);
		}
  });
};

Bucket.prototype.DELETEObject = function(key, callback) {
    var _this = this;
    process.nextTick(function() {
      delete _this.keyMap[key];
      if(callback) {
        callback();
      }
    });
};

Bucket.prototype.DELETEBucket = function(callback){
	if(!callback || typeof callback !== "function"){
		throw Error("Need callback function.");
	}
	//Per AWS, all objects and delete markers must be deleted first.
	process.nextTick(function(){
		//placeholder for actual delete function
		callback();
	});
};


Bucket.prototype.GETBucketListObjects = function(prefix, marker, delimiter, maxKeys, callback){
	if(prefix && typeof prefix !== "string"){
		return callbcack("Prefix must be a string", null);
	}

	if(marker && typeof marker !== "string"){
		return callback("Marker must be a string", null);
	}

	if(delimiter && typeof delimiter !== "string"){
		return callback("Delimeter must be a string", null);
	}

	if(maxKeys && typeof maxKeys !== "number"){
		return callback("MaxKeys must be a number", null);
	}


	var _this = this;
	var response = new ListBucketResult();
	maxKeys = maxKeys || DEFAULT_MAX_KEYS;
	var keys = Object.keys(_this.keyMap).sort();


	//If marker specified, edit the keys array so it only contains keys that occur alphabetically after the marker
	if(marker){
		keys = markerFilter(marker, keys);
		response.Marker = marker;
	}


	//If prefix specified, edit the keys array so it only contains keys that contain the prefix
	if(prefix){
		keys = prefixFilter(prefix, keys);
		response.Prefix = prefix;
	}


	//Iterate through keys array and filter keys containing delimeter into response.CommonPrefixes and filter remaining keys into response.Contents
	var keys_length = keys.length;
	for(var i=0; i<keys_length; i++){
		var current_key = keys[i];

		//If hit maxKeys, stop adding keys to response
		if(response.MaxKeys >= maxKeys){
			response.IsTruncated = true;
			response.NextMarker = findNextMarker(i, keys, response);
			break;
		}

		//If a delimiter is specified, find its index in the current key
    var delimiter_index;
		if(delimiter){
			delimiter_index = current_key.indexOf(delimiter);
			response.Delimiter = delimiter;
		} else{
			delimiter_index = -1;
		}

		//If `delimeter` occurs in current key, add key to response.CommonPrefixes.  Otherwise add key to response.Contents
		if(delimiter_index > -1){
			var key_substring = current_key.slice(0, delimiter_index + 1);
			response.addCommonPrefix(key_substring);
		} else {
			response.addContentsKey(current_key, _this.keyMap);
		}
	}

	callback(null, response);

};


module.exports = Bucket;
