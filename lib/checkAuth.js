

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
	} else if(req.query.Signature){
		return "v2QueryAuth"

		//Handle v4?


	} else {
		//Not signed
		return "none";

	}

};


var _runParticularAuthCheck = function(authType, request, callback){
	if(authType === "v2HeaderAuth"){

	}
	else if(authType === "v2QueryAuth"){

	} else {
		
	}


};


var checkAuth = function(request, response, callback){

	var authType = _determineAuthType(request);

	if(authType === "none"){
		callback("No authentication provided");
	} else {
		_runParticularAuthCheck(authType, request, callback);
	}

};

module.exports = checkAuth;


