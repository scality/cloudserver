var crypto = require("crypto");
var url = require("url");
var utf8 = require("utf8");
var secretKeys = {
	"accessKey1": "verySecretKey1",
	"accessKey2": "verySecretKey2"
};



_hashSignature = function(stringToSign, secretKey, algorithm){
	var utf8stringToSign = utf8.encode(stringToSign);
	var hmacObject = crypto.createHmac(algorithm, secretKey);
	var hashedSignature = hmacObject.update(utf8stringToSign).digest('base64');

	return hashedSignature;

};

_getCanonicalizedAmzHeaders = function(headers){
	var headerString = "";

	var amzHeaders = [];

	//Iterate through request.headers and pull any headers that are x-amz headers.
	//Originally, tried excluding "x-amz-date" here because AWS docs indicate this should be used for the
	//date header in the string to sign.  However, in practice, need x-amz-date in amzHeaders.

	for(var key in headers){
		if(key.substr(0, 6) === "x-amz-"){
			amzHeaders.push([key.trim(), headers[key].trim()]);
		}
	};

	//If there are no amz headers, just return an empty string
	if(amzHeaders.length === 0){
		return headerString;
	}

	//If there is only one amz header, return it in the proper format
	if(amzHeaders.length === 1){
		return amzHeaders[0][0] + ":" + amzHeaders[0][1] + "\n";
	}

	//Sort the amz headers by key (first item in tuple)
	amzHeaders.sort(function(a, b){return a[0] > b[0]});

	//Iterate over amzHeaders array and combine any items with duplicate headers into same tuple with values comma separated
	//NOTE: s3cmd will not allow duplicate headers so combining duplicates has not been tested.  

	var amzHeadersNoDups = [[amzHeaders[0][0], amzHeaders[0][1]]];

	for(var i=1; i<amzHeaders.length; i++){
		//Check if current header name is the same as the last one added to amzHeadersNoDups.  If it is, add to the content
		//to the tuple already in amzHeadersNoDups.  Otherwise, add a new tuple to amzHeadersNoDups
		if(amzHeaders[i][0] === amzHeadersNoDups[amzHeadersNoDups.length-1][0]){
			amzHeadersNoDups[amzHeadersNoDups.length-1][1] = amzHeadersNoDups[amzHeadersNoDups.length-1][1] + "," + amzHeaders[i][1];
		} else{
			amzHeadersNoDups.push(amzHeaders[i]);
		}
	}

	//Build headerString

	for(var j=0; j<amzHeadersNoDups.length; j++){
			headerString += amzHeadersNoDups[j][0] + ":" + amzHeadersNoDups[j][1] + "\n";
	}


	return headerString;

};

_getCanonicalizedResource = function(request){
	var resourceString = "";

	//This variable is used to determine whether to insert a "?" or "&".  Once have added a query parameter to the resourceString,
	//switch haveAddedQueries to true and add "&" before any new query parameter.
	var haveAddedQueries = false;

	//If bucket specified in hostname, add to resourceString
	var host = request.headers.host;
	var hostArray = host.split(".");
	//If first part of host is s3 or 127 for localhost, exclude it.  
	if(hostArray[0] !== "s3" && hostArray[0] != "127"){
		resourceString += hostArray[0];
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

  
   var presentSubresources = [];

   //Check which specified subresources are present in query string, build array with them
   for(var param in request.query){
   	//If need to pick up existing query parameters that do not have values, add "|| subresources.indexOf(param + "/")" to conditional below
   	if(subresources.indexOf(param) > -1){
   		presentSubresources.push(param);
   	}
   }

   console.log("request.query", request.query);
   console.log("presentSubresources", presentSubresources);
   console.log("request.url", request.url)

   //Sort the array and add the subresources and their value (if any) to the resourceString
   if(presentSubresources.length > 0){
   	presentSubresources.sort();
   	for(var i=0; i< presentSubresources.length; i++){
   		resourceString += (i === 0 ? "?" : "&");
   		var subresource = presentSubresources[i];
   		resourceString += subresource;
   		haveAddedQueries = true;
   		if(request.query[subresource] !== ""){
   			resourceString += "=" + request.query[subresource];
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
   	if(request.query[overridingParams[j]]){
   		//Addresses adding "?" instead of "&" if no subresources added.
   		resourceString += (haveAddedQueries ? "&" : "?");
   		resourceString += overridingParams[j] + "=" + request.query[overridingParams[j]];
   		haveAddedQueries = true;
   	}
   }


   // Per AWS, the delete query string parameter must be included when
   // you create the CanonicalizedResource for a multi-object Delete request.  Does this mean should be
   //excluded if single item delete request?  How determine?

   if(request.query["delete"]){
   	//Addresses adding "?" instead of "&" if no other params added.
   	resourceString += (haveAddedQueries ? "&" : "?");
   	resourceString += "delete=" + request.query["delete"];
   }

	return resourceString;

};

_reconstructSignature = function(secretKey, request){

//Build signature per AWS requirements:

/*StringToSign = HTTP-Verb + "\n" +
	Content-MD5 + "\n" +
	Content-Type + "\n" +
	Date (or Expiration for query Auth) + "\n" +
	CanonicalizedAmzHeaders +
	CanonicalizedResource;*/

	var stringToSign = request.method + "\n";
	stringToSign += (request.lowerCaseHeaders['content-md5'] ? request.lowerCaseHeaders['content-md5'] + '\n' : '\n');
	stringToSign += (request.lowerCaseHeaders['content-type'] ? request.lowerCaseHeaders['content-type'] + '\n' : '\n');

	//AWS docs are conflicting on whether to include x-amz-date header here if present in request.
	//s3cmd includes x-amz-date in amzHeaders rather than here so I have replicated that.
	var date = request.lowerCaseHeaders['date'] || request.query['Expiration'];
	stringToSign += (date ? date + '\n' : '\n');
	stringToSign += _getCanonicalizedAmzHeaders(request.lowerCaseHeaders);
	stringToSign += _getCanonicalizedResource(request);

	console.log("stringToSign", stringToSign)

	var hashedSignature = _hashSignature(stringToSign, secretKey, "sha1");

	return hashedSignature;

};



_checkSignatureMatch = function(secretKey, signature, request, callback){
	var reconstructedSignature = _reconstructSignature(secretKey, request);
	console.log("my sig", reconstructedSignature);
	console.log("provided sig", signature);

	if(reconstructedSignature === signature){
		//Determine whether we should be sending back some user info here.
		return callback(null, "Success!");
	} else{
		return callback("Access denied.")
	}

};

_getSecretKey = function(accessKey, signature, request, callback){
	//Retrieve secret key based on accessKey.
  process.nextTick(function() {
		var secretKey = secretKeys[accessKey];
		if(!secretKey){
			//Error message specificity to be discussed.
			return callback("Error retrieving key.");
		} else {
			_checkSignatureMatch(secretKey, signature, request, callback);
		}

  });

};


_checkTimestamp = function(timestamp){
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
	var timestamp = request.lowerCaseHeaders['x-amz-date'] || request.lowerCaseHeaders['date'];

	if(!timestamp){
		return callback("Missing date header");
	}

	var timeOut = _checkTimestamp(timestamp);

	if(timeOut){
		return callback("RequestTimeTooSkewed: The difference between the request time and the current time is too large.")
	}


	//Authorization Header should be in the format of "AWS AccessKey:Signature"
	var authorizationInfo = request.lowerCaseHeaders.authorization;

	if(!authorizationInfo){
		//Error message specificity to be discussed.
		return callback("Missing header authorization information.");
	}
	var semicolonIndex = authorizationInfo.indexOf(":");

	if(semicolonIndex < 0){
		//Error message specificity to be discussed.
		return callback("Header autorization information incomplete");
	}
	var accessKey = authorizationInfo.substring(4, semicolonIndex).trim();
	var signature = authorizationInfo.substring(semicolonIndex +1).trim();


	_getSecretKey(accessKey, signature, request, callback);

};



_v2QueryAuthCheck = function(request, callback){
	
	if(request.method === "POST"){
		return callback("Query string authentication is not supported for POST.");
	}

	//Check whether request has expired
	var expirationTime = request.query["Expires"];
	var currentTime = Date.now();
	if(currentTime > expirationTime){
		return callback("RequestTimeTooSkewed: The difference between the request time and the current time is too large.")
	}

	var accessKey = request.query["AWSAccessKeyId"];
	var signature = request.query["Signature"];

	if(!accessKey || !signature){
		return callback("Missing query authorization information.");
	}

	_getSecretKey(accessKey, signature, request, callback);

};



var _runParticularAuthCheck = function(request, callback){
	var authHeader = request.lowerCaseHeaders.authorization;

	//Check whether signature is in header
	if(authHeader){

		//Check for security token header to handle temporary security credentials?

		//Check if v2
		if(authHeader.substr(0,4) === "AWS "){
			_v2HeaderAuthCheck(request, callback);
		} else {
			//Deal with v4HeaderAuth
			console.log("V4HeaderAuth");
		}


		//Check whether signature is in query string
	} else if(request.query.Signature){
		_v2QueryAuthCheck(request, callback);

	} else if(request.query["X-Amz-Algorithm"]){
				//Handle v4 query scenario

	} else {
		//Not signed
		return callback("Authentication information is inadequate.")

	}

};


var checkAuth = function(request, response, callback){

	//Add to request object an object that contains the headers with all lowercase keys
	request.lowerCaseHeaders = {};

	for(var key in request.headers){
		request.lowerCaseHeaders[key.toLowerCase()] = request.headers[key];
	}

	//Add to request object, an object containing the query parameters
	request.query = url.parse(request.url, true).query;

	_runParticularAuthCheck(request, callback);

};

module.exports = checkAuth;
