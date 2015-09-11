var utilities = require("./bucket_utilities.js");
var markerFilter = utilities.markerFilter;
var prefixFilter = utilities.prefixFilter;
var async = require("async");
var DEFAULT_MAX_KEYS = 1000;

var ListBucketResult = function() {
    this.error = false;
    this.is_truncated = false;
    this.next_marker = undefined;
    //Need to modify tests to conform.  
    this.common_prefixes = [];
    this.contents = [];
    this.key_count = 0;
};

ListBucketResult.prototype.errorMessage = function(reason) {
    this.error = true;
    this.reason = reason;
};


ListBucketResult.prototype.addContentsKey = function(key) {
  this.contents.push({"key": key});
  this.key_count++;
};

ListBucketResult.prototype.addCommonPrefix = function(prefix) {
  if(!this.hasCommonPrefix(prefix)) {
		this.common_prefixes.push(prefix);
		this.key_count++;
  }
};

ListBucketResult.prototype.hasCommonPrefix = function(prefix) {
  return (this.common_prefixes.indexOf(prefix) !== -1);
};

var Bucket = function() {
    this.keyMap = {};
};


Bucket.prototype.PUTObject = function(key, value, callback) {
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
	};
	//Per AWS, all objects and delete markers must be deleted first. 
	process.nextTick(function(){
		//placeholder for actual delete function
		callback();
	}); 
};


Bucket.prototype.GETBucketListObjects = function(prefix, marker, delimiter, maxKeys, callback){
	if(!callback || typeof callback !== "function"){
		throw("Callback function required.");
	};

	if(prefix && typeof prefix !== "string"){
		throw("Prefix must be a string.");
	};

	if(marker && typeof marker !== "string"){
		throw("Marker must be a string.");
	};

	if(delimiter && typeof delimiter !== "string"){
		throw("Delimeter must be a string.");
	};

	if(maxKeys && typeof maxKeys !== "number"){
		throw("MaxKeys must be a number");
	};


	var _this = this;
	var response = new ListBucketResult();
	var maxKeys = maxKeys || DEFAULT_MAX_KEYS;
	var keys = Object.keys(_this.keyMap).sort();

	
	//If marker specified, edit the keys array so it only contains keys that occur alphabetically after the marker
	if(marker){
		keys = markerFilter(marker, keys);
	};


	//If prefix specified, edit the keys array so it only contains keys that contain the prefix
	if(prefix){
		keys = prefixFilter(prefix, keys);
	};


	//Iterate through keys array and filter keys containing delimeter into response.common_prefixes and filter remaining keys into response.contents
	var keys_length = keys.length;
	for(var i=0; i<keys_length; i++){
		var current_key = keys[i];

		//If hit maxKeys, stop adding keys to response
		if(response.key_count >= maxKeys){
			response.is_truncated = true;
			response.next_marker = current_key;
			break;
		}

		//If a delimiter is specified, find its index in the current key
		if(delimiter){
			var delimiter_index = current_key.indexOf(delimiter);
		} else{
			var delimiter_index = -1;
		}

		//If delimeter occurs in current key, add key to response.common_prefixes.  Otherwise add key to response.contents
		if(delimiter_index > -1){
			var key_substring = current_key.slice(0, delimiter_index + 1);
			response.addCommonPrefix(key_substring);
		} else {
			response.addContentsKey(current_key);
		};
	};


//Iterate through response.contents to get value from the keymap and set it in response.contents.  
//Then call the callback with the response object.
	async.forEachOf(response.contents, function(item, index, next){
		_this.GETObject(item.key, function(err, value, key){
			if(err){
				//Handle this error differently?
				response.errorMessage("Was not able to retrive key: " + key);
			} else {
				response.contents[index].value = value;
				next();
			}
		})
	}, function(err){
		if(err){
			response.errorMessage("Error: " + err);
		}
		callback(response);
	});

};


module.exports = Bucket;
