var crypto = require("crypto");
var url = require("url");
var utf8 = require("utf8");
// [[RESTORE THIS]]
// var getBucketNameFromHost = require("../utilities.js").getBucketNameFromHost;
var secretKeys = require("../testdata/vault.json").secretKeys;

var Auth = {}


Auth._hashSignature = function(stringToSign, secretKey, algorithm){
	var utf8stringToSign = utf8.encode(stringToSign);
	var hmacObject = crypto.createHmac(algorithm, secretKey);
	var hashedSignature = hmacObject.update(utf8stringToSign).digest('base64');

	return hashedSignature;

};

Auth._getCanonicalizedAmzHeaders = function(headers){
	var headerString = "";

	var amzHeaders = [];

	//Iterate through headers and pull any headers that are x-amz headers.
	//Originally, tried excluding "x-amz-date" here because AWS docs indicate this should be used for the
	//date header in the string to sign.  However, in practice, need x-amz-date in amzHeaders.


	for(var key in headers){
		if(key.substr(0, 6) === "x-amz-"){
			var value = headers[key].trim();

			//AWS docs state that duplicate headers should be combined in the same header with values concatenated with
			//a comma separation.  Node combines duplicate headers and concatenates the values with a comma AND SPACE separation.
			//The following code would remove that space, but it is too broad as it would also remove the space
			//in a date value.  Opted to proceed without this parsing since it does not appear that the AWS clients use
			//duplicate headers.

			// if(value.indexOf(", ") > -1){
			// 	value = value.replace(/, /, ",");
			// }
			amzHeaders.push([key.trim(), value]);
		}
	};

	//If there are no amz headers, just return an empty string
	if(amzHeaders.length === 0){
		return headerString;
	}

	//Sort the amz headers by key (first item in tuple)
	if(amzHeaders.length > 1){
		amzHeaders.sort(function(a, b){return a[0] > b[0]});
	}


	//Build headerString

	for(var j=0; j<amzHeaders.length; j++){
			headerString += amzHeaders[j][0] + ":" + amzHeaders[j][1] + "\n";
	}


	return headerString;

};

Auth._getCanonicalizedResource = function(request){
	var resourceString = "";

	//This variable is used to determine whether to insert a "?" or "&".  Once have added a query parameter to the resourceString,
	//switch haveAddedQueries to true and add "&" before any new query parameter.
	var haveAddedQueries = false;

	//If bucket specified in hostname, add to resourceString

/*	[[RESTORE THIS]]
	var bucket = getBucketNameFromHost(request);
	resourceString += bucket;*/

	//Add the path to the resourceString
	resourceString += url.parse(request.url).pathname

	//If request includes a specified subresource, add to the resourceString: (a) a "?", (b) the subresource, and (c) its value (if any).
	//Separate multiple subresources with "&".  Subresources must be in alphabetical order.

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

Auth._reconstructSignature = function(secretKey, request){

//Build signature per AWS requirements:

/*StringToSign = HTTP-Verb + "\n" +
	Content-MD5 + "\n" +
	Content-Type + "\n" +
	Date (or Expiration for query Auth) + "\n" +
	CanonicalizedAmzHeaders +
	CanonicalizedResource;*/

	var stringToSign = request.method + "\n";

	var contentMD5 = request.lowerCaseHeaders['content-md5'] || request.query['Content-MD5'];
	stringToSign += (contentMD5 ? contentMD5 + '\n' : '\n');

	var contentType = request.lowerCaseHeaders['content-type'] || request.query['Content-Type'];
	stringToSign += (contentType ? contentType + '\n' : '\n');

	//AWS docs are conflicting on whether to include x-amz-date header here if present in request.
	//s3cmd includes x-amz-date in amzHeaders rather than here so I have replicated that.
	var date = request.lowerCaseHeaders['date'] || request.query['Expires'];
	stringToSign += (date ? date + '\n' : '\n');
	stringToSign += Auth._getCanonicalizedAmzHeaders(request.lowerCaseHeaders);
	stringToSign += Auth._getCanonicalizedResource(request);

	var hashedSignature = Auth._hashSignature(stringToSign, secretKey, "sha1");

	return hashedSignature;

};


Auth._checkSignatureMatch = function(accessKey, secretKey, signature, request, callback){
	var reconstructedSignature = Auth._reconstructSignature(secretKey, request);

	if(reconstructedSignature === signature){
		return callback(null, accessKey);
	}
	return callback("Access denied.")
};

Auth._getSecretKey = function(accessKey, signature, request, callback){
	//Retrieve secret key based on accessKey.
  process.nextTick(function() {
		var secretKey = secretKeys[accessKey];
		if(!secretKey){
			//Error message specificity to be discussed.
			return callback("Error retrieving key.");
		}
		Auth._checkSignatureMatch(accessKey, secretKey, signature, request, callback);
  });

};


Auth._checkTimestamp = function(timestamp){
	//If timestamp is more than 15 minutes old, return true
	var currentTime = Date.now();
	var requestTime = Date.parse(timestamp);
	var fifteenMinutes = (15 * 60 *1000);

	if((currentTime - requestTime) > fifteenMinutes){
		return true;
	}
	return false;
};

Auth._v2HeaderAuthCheck = function(request, callback){
	//Check to make sure timestamp is less than 15 minutes old
	var timestamp = request.lowerCaseHeaders['x-amz-date'] || request.lowerCaseHeaders['date'];

	if(!timestamp){
		return callback("Missing date header");
	}

	var timeOut = Auth._checkTimestamp(timestamp);

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


	Auth._getSecretKey(accessKey, signature, request, callback);

};

Auth._v2QueryAuthCheck = function(request, callback){

	if(request.method === "POST"){
		return callback("Query string authentication is not supported for POST.");
	}

	// Check whether request has expired.  Expires time is provided in seconds so need to multiply by 1000 to obtain
	//milliseconds to compare to Date.now()
	var expirationTime = parseInt(request.query["Expires"]) * 1000;
	var currentTime = Date.now();
	if(currentTime > expirationTime){
		return callback("RequestTimeTooSkewed: The difference between the request time and the current time is too large.")
	}

	var accessKey = request.query["AWSAccessKeyId"];
	var signature = request.query["Signature"];

	if(!accessKey || !signature){
		return callback("Missing query authorization information.");
	}

	Auth._getSecretKey(accessKey, signature, request, callback);

};

Auth._runParticularAuthCheck = function(request, callback){
	var authHeader = request.lowerCaseHeaders.authorization;

	//Check whether signature is in header
	if(authHeader){

		//Check for security token header to handle temporary security credentials?

		//Check if v2
		if(authHeader.substr(0,4) === "AWS "){
			Auth._v2HeaderAuthCheck(request, callback);
		} else {
			//Deal with v4HeaderAuth
			console.log("V4HeaderAuth");
		}


		//Check whether signature is in query string
	} else if(request.query.Signature){
		Auth._v2QueryAuthCheck(request, callback);

	} else if(request.query["X-Amz-Algorithm"]){
				//Handle v4 query scenario

	} else {
		//Not signed
		return callback("Authentication information is inadequate.")

	}

};


Auth.checkAuth = function(request, callback){

	//Add to request object an object that contains the headers with all lowercase keys
	request.lowerCaseHeaders = {};

	for(var key in request.headers){
		request.lowerCaseHeaders[key.toLowerCase()] = request.headers[key];
	}


	//Add to request object, an object containing the query parameters
	request.query = url.parse(request.url, true).query;


	Auth._runParticularAuthCheck(request, callback);

};

module.exports = Auth;
