

var _determineAuthType = function(request){
	var authHeader = request.headers.authorization;

	//Check whether signature is in header
	if(authHeader){

		//Check for security token header?

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

_getSecretKey = function(accessKey, signature, request, callback){
	//Continue here and pull secret key based on accessKey.  

}

_v2HeaderAuthCheck = function(request, callback){
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


