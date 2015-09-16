var crypto = require("crypto");
var url = require("url");
var secretKeys = { 
	"accessKey1": "verySecretKey1",
	"accessKey2": "verySecretKey2"
};

var _determineAuthType = function(request){
	var authHeader = request.lowerCaseHeaders.authorization;

	//Check whether signature is in header
	if(authHeader){

		//Check for security token header to handle temporary security credentials?

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


_hashSignature = function(stringToSign, secretKey, algorithm){
	var utf8stringToSign = encodeURIComponent(stringToSign);

	//If need stricture utf8 encoding, per JS docs, use:
/*	function fixedEncodeURIComponent (str) {
	  return encodeURIComponent(str).replace(/[!'()*]/g, function(c) {
	    return '%' + c.charCodeAt(0).toString(16);
	  });
	}*/


	var hmacObject = crypto.createHmac(algorithm, secretKey);
	var hashedSignaure = hmacObject.update(utf8stringToSign).digest('base64');
	
	return hashedSignature;

};

_getCanonicalizedAmzHeaders = function(headers){
	var headerString = "";

	var amzHeaders = [];

	//Iterate through request.headers and pull any headers that are x-amz headers except for 
	//the date header which is dealt with explicitly in stringToSign
	for(var key in headers){
		if(key.substr(0, 6) === "x-amz-" && key !== "x-amz-date"){
			amzHeaders.push([key.trim(), headers[key]].trim());
		}
	};

	//If there are no amz headers, just return an empty string
	if(amzHeaders.length === 0){
		return headerString;
	}

	//If there is only one amz header, return it in the proper format
	if(amzHeaders.length === 1){
		return amzHeaders[0][0] + ":" + amzHeaders[0][1];
	}

	//Sort the amz headers by key (first item in tuple)
	amzHeaders.sort(function(a, b){return a[0] > b[0]});

	//Iterate over amzHeaders array and combine any items with duplicate headers into same tuple with values comma separated
	for(var i=1; i<amzHeaders.length; i++){
		if(amzHeaders[i][0] === amzHeaders[i-1][0]){
			amzHeaders[i-1][1] = amzHeaders[i-1][1] + "," + amzHeaders[i][1];
		}
		amzHeaders[i] = undefined;
	}

	//Build headerString

	for(var j=0; j<amzHeaders.length; j++){
		if(amzHeaders[j]){
			headerString += amzHeaders[j][0] + ":" + amzHeaders[j][1] + "\n";
		}
	}

	//Return headerString without the last line break
	return headerString.substr(0, headerString.length -2);

};

_getCanonicalizedResource = function(request){
	//Need to finish this
	var resourceString = "";

	//If bucket specified in host header, add to resourceString (Need to confirm this results in correct format) 
	if(request.lowerCaseHeaders.host){
		resourceString += "/" + request.lowerCaseHeaders.host;
	}


	//Add the path to the resourceString
	resourceString += url.parse(request.url).pathname

	//If request includes a specified subresource, add to the resourceString: (a) the subresource, (b) its value (if any)
	// and (c) a "?".  Separate multiple subresources with "&".  Subresources must be in alphabetical order.

	//Specified subresources: 
	var subresources = [
    "acl",
    "lifecycle",
    "location",
    "logging",
    "notification",
    "partNumber",
    "policy",
    "requestPayment",
    "torrent",
    "uploadId",
    "uploads",
    "versionId",
    "versioning",
    "versions",
    "website"];

   var queryObject = url.parse(req.url,true).query;

   var presentSubresources = [];

   //Check which specified subresources are present in query string, build array with them
   for(var param in queryObject){
   	if(subresources.indexOf[param] > -1){
   		presentSubresources.push(param);
   	}
   }

   //Sort the array and add the subresources and their value (if any) to the resourceString
   if(presentSubresources.length > 0){
   	presentSubresources.sort();
   	for(var i=0; i< presentSubresources.length; i++){
   		resourceString += (i === 0 ? "?" : "&");
   		var subresource = presentSubresources[i];
   		resourceString += subresource;
   		if(queryObject[subresource] !== ""){
   			resourceString += queryObject[subresource];
   		}
   	}
   }

   //If the request includes in the query string, parameters that override the headers, include them in the resourceString
   //along with their values.  AWS is ambiguous about format.  Alphabetical order?  

   var overridingParams = [
    "response-cache-control",
   	"response-content-disposition",
   	"response-content-encoding",
   	"response-content-language",
   	"response-content-type",
   	"response-expires",
   ];

   for(var j=0; j< overridingParams.length; j++){
   	if(queryObject[overridingParams[j]]){
   		//Need to address adding "?" instead of "&" if no subresources added.
   		resourceString += "&";
   		resourceString += overridingParams[j] + "=" + queryObject[overridingParams[j]];
   	}
   }


   // Per AWS, the delete query string parameter must be included when 
   // you create the CanonicalizedResource for a multi-object Delete request.  Does this mean should be 
   //excluded if single item delete request?  How determine?

   if(queryObject["delete"]){
   	//Need to address adding "?" instead of "&" if no other params added.
   	resourceString += "&";
   	resourceString += "delete=" + queryObject["delete"];
   }

	return resourceString;

};

_reconstructSignature = function(secretKey, request){

//Build signature per AWS requirements:

/*StringToSign = HTTP-Verb + "\n" +
	Content-MD5 + "\n" +
	Content-Type + "\n" +
	Date + "\n" +
	CanonicalizedAmzHeaders +
	CanonicalizedResource;*/

	var stringToSign = request.method + "\n";
	stringToSign += (request.lowerCaseHeaders['content-md5'] ? request.lowerCaseHeaders['content-md5'] + '\n' : '\n');
	stringToSign += (request.lowerCaseHeaders['content-type'] ? request.lowerCaseHeaders['content-type'] + '\n' : '\n');

	//For date, use date specified in HTTP date header or x-amz-date header
	var date = request.lowerCaseHeaders['date'] || request.lowerCaseHeaders['x-amz-date'];
	stringToSign += (date ? date + '\n' : '\n');

	stringToSign += getCanonicalizedAmzHeaders(request.lowerCaseHeaders);
	stringToSign += getCanonicalizedResource(request); 


	var hashedSignature = _hashSignature(stringToSign, secretKey, "sha1");

	return hashedSignature;

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
	var timeStamp = request.lowerCaseHeaders['x-amz-date'] || request.lowerCaseHeaders[date];

	if(!timeStamp){
		callback("Missing date header");
	}

	var timeOut = _checkTimeStamp(timeStamp);

	if(timeOut){
		callback("RequestTimeTooSkewed: The difference between the request time and the current time is too large.")
	}


	//Authorization Header should be in the format of "AWS AccessKey:Signature"
	var authorizationInfo = request.lowerCaseHeaders.authorization;

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
	//Need to complete


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

	//Add to request object an object that contains the headers with all lowercase keys
	request.lowerCaseHeaders = {};

	for(var key in request.headers){
		request.lowerCaseHeaders[key.toLowerCase()] = request[key];
	}

	var authType = _determineAuthType(request);

	if(authType === "none"){
		//Error message specificity to be determined
		callback("No authentication provided");
	} else {
		_runParticularAuthCheck(authType, request, callback);
	}

};

module.exports = checkAuth;


