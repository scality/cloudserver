var secretKeys = { 
	"accessKey1": "verySecretKey1",
	"accessKey2": "verySecretKey2"
};

var _determineAuthType = function(request){
	var authHeader = request.headers.authorization;

	//Check whether signature is in header
	if(authHeader){

		//Check for security token header (other AWS path)?

		//Check if v2
		if(authHeader.substr(0,4) === "AWS "){
			return "v2HeaderAuth";
		} else {
			//Deal with v4HeaderAuth
			console.log("V4HeaderAuth");
		}


		//Check whether signature is in query string
	} else if(request.query && request.query.Signature){
		return "v2QueryAuth"

		//Handle v4?


	} else {
		//Not signed
		return "none";

	}

};




_checkSignatureMatch = function(secretKey, signature, request, callback){
	var reconstructedSignature = _reconstructSignature(secretKey, request);


	if(reconstructedSignature === signature){
		//Determine whether we should be sending back some user info here.  
		callback(null, "Success!");
	} else{ 
		callback("Access denied.")
	}

};

_getSecretKey = function(accessKey, signature, request, callback){
	//Retrieve secret key based on accessKey.  
  process.nextTick(function() {
		var secretKey = secretKeys[accessKey];
		if(!secretKey){
			//Error message specificity to be discussed.
			callback("Error retrieving key.");
		} else {
			_checkSignatureMatch(secretKey, signature, request, callback);
		}
	
  });

};


_checkTimeStamp = function(timeStamp){
	//If timestamp is more than 15 minutes old, return true
	var currentTime = Date.now();
	var requestTime = Date(timestamp);
	var fifteenMinutes = (15 * 60 *1000);

	if((currentTime - requestTime) > fifteenMinutes){
		return true;
	} else {
		return false;
	}
};

_v2HeaderAuthCheck = function(request, callback){
	//Check to make sure timestamp is less than 15 minutes old
	var timeStamp = request.headers['x-amz-date'] || request.headers[date];

	if(!timeStamp){
		callback("Missing date header");
	}

	var timeOut = _checkTimeStamp(timeStamp);

	if(timeOut){
		callback("RequestTimeTooSkewed: The difference between the request time and the current time is too large.")
	}


	//Authorization Header should be in the format of "AWS AccessKey:Signature"
	var authorizationInfo = request.headers.authorization;

	if(!authorizationInfo){
		//Error message specificity to be discussed.
		callback("Missing header authorization information.");
	}
	var semicolonIndex = authorizationInfo.indexOf(":");

	if(semicolonIndex < 0){
		//Error message specificity to be discussed.
		callback("Header autorization information incomplete");
	}
	var accessKey = authorizationInfo.substring(4, semicolonIndex).trim();
	var signature = authorizationInfo.substring(semicolonIndex).trim();


	_getSecretKey(accessKey, signature, request, callback);

};



_v2QueryAuthCheck = function(request, callback){



};


var _runParticularAuthCheck = function(authType, request, callback){
	if(authType === "v2HeaderAuth"){
		_v2HeaderAuthCheck(request, callback);

	}
	else if(authType === "v2QueryAuth"){
		_v2QueryAuthCheck(request, callback);

	} else {
		//Deal with v4


		//Error message specificity to be determined
		callback("Authentication information is inadequate.")
	}


};


var checkAuth = function(request, response, callback){

	var authType = _determineAuthType(request);

	if(authType === "none"){
		//Error message specificity to be determined
		callback("No authentication provided");
	} else {
		_runParticularAuthCheck(authType, request, callback);
	}

};

module.exports = checkAuth;


