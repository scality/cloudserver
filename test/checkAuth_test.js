var chai = require("chai");
var expect = chai.expect;
var Auth = require("../lib/auth/checkAuth.js")

//Add tests for:

//canonicalized resource

//canonicalized header

//timeout

//stringToSign


describe("Auth._reconstructSignature",function(){


  it("should reconstruct the signature for a GET request", function(){
  	//Based on s3-curl run
  	var REQUEST = {
  		method: "GET",
  		headers: { host: 's3.amazonaws.com',
  		  'user-agent': 'curl/7.43.0',
  		  accept: '*/*',
  		  date: 'Fri, 18 Sep 2015 22:57:23 +0000',
  		  authorization: 'AWS accessKey1:MJNF7AqNapSu32TlBOVkcAxj58c=' },
  		url: "/bucket",
  		lowerCaseHeaders: {
  			date: 'Fri, 18 Sep 2015 22:57:23 +0000',
  		},
  		query: {}

  	};
  	var SECRET_KEY = "verySecretKey1";

  	var reconstructedSig = Auth._reconstructSignature(SECRET_KEY, REQUEST);
  	expect(reconstructedSig).to.equal("MJNF7AqNapSu32TlBOVkcAxj58c=");
  });


  it("should reconstruct the signature for a PUT request", function(){
  	//Based on s3cmd run
  	var REQUEST = {
  		method: "PUT",
  		headers: { host: '127.0.0.1:8000',
			'accept-encoding': 'identity',
			authorization: 'AWS accessKey1:fWPcicKn7Fhzfje/0pRTifCxL44=',
			'content-length': '3941',
			'content-type': 'binary/octet-stream',
			'x-amz-date': 'Fri, 18 Sep 2015 23:32:34 +0000',
			'x-amz-meta-s3cmd-attrs': 'uid:501/gname:staff/uname:lhs/gid:20/mode:33060/mtime:1319136702/atime:1442619138/md5:5e714348185ffe355a76b754f79176d6/ctime:1441840220',
			'x-amz-now': 'susdr',
			'x-amz-y': 'what' },
  		url: "/test/obj",
  		lowerCaseHeaders: { host: '127.0.0.1:8000',
			  'accept-encoding': 'identity',
			  authorization: 'AWS accessKey1:fWPcicKn7Fhzfje/0pRTifCxL44=',
			  'content-length': '3941',
			  'content-type': 'binary/octet-stream',
			  'x-amz-date': 'Fri, 18 Sep 2015 23:32:34 +0000',
			  'x-amz-meta-s3cmd-attrs': 'uid:501/gname:staff/uname:lhs/gid:20/mode:33060/mtime:1319136702/atime:1442619138/md5:5e714348185ffe355a76b754f79176d6/ctime:1441840220',
			  'x-amz-now': 'susdr',
			  'x-amz-y': 'what' },
  		query: {}

  	};
  	var SECRET_KEY = "verySecretKey1";

  	var reconstructedSig = Auth._reconstructSignature(SECRET_KEY, REQUEST);
  	expect(reconstructedSig).to.equal("fWPcicKn7Fhzfje/0pRTifCxL44=");
  });


});

