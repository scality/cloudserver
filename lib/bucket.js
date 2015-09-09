var DEFAULT_MAX_KEYS = 1000;

var ListBucketResult = function() {
    this.error = false;
    this.attrs = {};
    this.fetched = {};
};

ListBucketResult.prototype.errorMessage = function(reason) {
    this.error = true;
    this.reason = reason;
};

ListBucketResult.prototype.get = function(attr) {
    return this.attrs[attr];
};

ListBucketResult.prototype.set = function(attr, value) {
    this.attrs[attr] = value;
};

ListBucketResult.prototype.getAttrs = function() {
    return this.attrs;
};

ListBucketResult.prototype.toString = function() {
    return this.attrs.toString();
};

ListBucketResult.prototype.addFetched = function(key, value) {
    this.fetched[key] = value;
};

ListBucketResult.prototype.addCommonPrefix = function(prefix) {
    if (!this.attrs.common_prefixes) {
	this.attrs.common_prefixes = [];
    }
    if (!this.hasCommonPrefix(prefix)) {
	this.attrs.common_prefixes.push(prefix);
    }
};

ListBucketResult.prototype.hasCommonPrefix = function(prefix) {
    return (this.attrs.common_prefixes &&
            this.attrs.common_prefixes.indexOf(prefix) != -1);
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
	if(!callback){
		throw Error("Need callback");
	};
	//Per AWS, all objects and delete markers must be deleted first. 
	process.nextTick(function(){
		//placeholder for actual delete function
		callback();
	}); 
}

Bucket.prototype.GETBucketListObjects = function(prefix, marker, delimiter, maxKeys, callback) {
    var _this = this;
    var numQueued = 0;
    var prefix = (prefix) ? prefix : '';
    var marker = (marker) ? marker : '';
    var delimiter = (delimiter) ? delimiter : '';
    var maxKeys = (maxKeys) ? maxKeys : DEFAULT_MAX_KEYS;
    var trimmed_key;
    // answer section
    var num_keys = 0;
    var is_truncated = false;

    if (!callback)
	throw("Callback required");
    
    response = new ListBucketResult();
    // check if marker matches prefix
    if (marker && marker.indexOf(prefix) !== 0) {
	response.errorMessage('GETBucketListObjects: prefix/marker mismatch');
	callback(response);
	return;
    }
    var keys = Object.keys(_this.keyMap).sort();
    // search for marker in bucket. FIXME: not efficient
    var startIndex = 0;
    if (marker && !startIndex) {
	for (var i = 0; i < keys.length; i++) {
	    var key = keys[i];
	    if ((key.indexOf(prefix) === 0) &&
		(key.substring(prefix.length).indexOf(delimiter) != -1) &&
		(key.indexOf(marker) != -1)) {
		startIndex = Math.max(startIndex, i + 1);
	    }
	}
    }
    // retrieve keys
    for (var j = startIndex; j < keys.length; j++) {
	var match_key = keys[j];
	// check if key matches prefix
	if (prefix && match_key.indexOf(prefix) !== 0) {
	    continue;
	}
	// check if we exceed maxKeys
	if (num_keys == maxKeys) {
	    response.set('next_marker', trim_key);
	    is_truncated = true;
	    break;
	}
	// check if delimiter is present in key
	var delimiter_index = match_key.substring(prefix.length).indexOf(delimiter);
	if (delimiter_index != -1) {
	    delimiter_index += prefix.length;
	}
	if (delimiter && delimiter_index !== -1) {
	    var trim_key = match_key.substring(0, delimiter_index + delimiter.length);
	    if (!response.hasCommonPrefix(trim_key)) {
		response.addCommonPrefix(trim_key);
		num_keys++;
	    }
	} else { // key is not delimited - just fetch it
	    num_keys++;
	    numQueued++;
	    _this.GETObject(match_key, function(err, value, key) {
		response.addFetched(key, value);
		if(--numQueued === 0) {
		    response.set('truncated', is_truncated);
		    callback(response);
		}
	    });
	}
    }
};

module.exports = Bucket, ListBucketResult;
