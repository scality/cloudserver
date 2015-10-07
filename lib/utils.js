'use strict';

const parseXmlString = require('xml2js').parseString;
const url = require('url');
const crypto = require('crypto');
const UUID = require('node-uuid');

let utils = {};

const awsRegions = [
  {
    "endpoint": "s3.amazonaws.com",
    "region": "us-east-1"
  },
  {
    "endpoint": "s3-external-1.amazonaws.com",
    "region": "us-east-1"
  },
  {
    "endpoint": "s3.eu-west-1.amazonaws.com",
    "region": "eu-west-1"
  },
  {
    "endpoint": "s3.ap-southeast-1.amazonaws.com",
    "region": "ap-southeast-1"
  },
  {
    "endpoint": "s3.ap-southeast-2.amazonaws.com",
    "region": "ap-southeast-2"
  },
  {
    "endpoint": "s3.eu-central-1.amazonaws.com",
    "region": "eu-central-1"
  },
  {
    "endpoint": "s3.eu.central-1.amazonaws.com",
    "region": "eu-central-1"
  },
  {
    "endpoint": "s3.ap-northeast-1.amazonaws.com",
    "region": "ap-northeast-1"
  },
  {
    "endpoint": "s3.us-east-1.amazonaws.com",
    "region": "us-east-1"
  },
  {
    "endpoint": "s3.sa-east-1.amazonaws.com",
    "region": "sa-east-1"
  },
  {
    "endpoint": "s3.us-west-1.amazonaws.com",
    "region": "us-west-1"
  },
  {
    "endpoint": "ec2.us-west-2.amazonaws.com",
    "region": "us-west-2"
  },
  {
    "endpoint": "s3-us-gov-west-1.amazonaws.com",
    "region": "us-gov-west-1"
  },
  {
    "endpoint": "s3-fips-us-gov-west-1.amazonaws.com",
    "region": "us-gov-west-1"
  }
];

/**
* Get region
* @param {object} request - http request object
*/
utils.getRegion = function(request, callback) {
  let host, endpoint, region, i, ii, hostArr, websiteEndpoint;
  if(request.headers.host === undefined){
    callback("Host is undefined");
  }
  host = request.headers.host.split(':')[0];

  if(host === 's3.amazonaws.com') {
    return callback(null, 'us-east-1');
  }

  if(request.body) {
    parseXmlString(request.body, function(err, result) {
      if(err) {
        return callback('Unable to parse request body', null);
      }

      for(i = 0, ii = awsRegions.length; i  < ii; i++) {
        if(awsRegions[i].region === result) {
          return callback(null, result);
        }
      }
      return callback('Region is invalid', null);
    });
  }

  for(i = 0, ii = awsRegions.length; i  < ii; i++) {
    a = awsRegions[i];
    endpoint = a.endpoint;
    region = a.region;
    websiteEndpoint = 's3-website-' + a.region + '.amazonaws.com';

    if(host.indexOf(endpoint) !== -1 || host.indexOf(websiteEndpoint)) {
      return callback(null, region);
    }
  }

  return callback('Unable to set region', null);
}

/**
* Get bucket name and object name from the request
* @param {object} request - http request object
*/
utils.getResourceNames = function(request) {
  let nameFromHost = this.getBucketNameFromHost(request);
  return this.getNamesFromPath(request, nameFromHost);
}

/**
* Get bucket name from the request of a virtually hosted bucket
* @param {object} request - http request object
*/
utils.getBucketNameFromHost = function(request) {
  let host, hostname, endpoint, region, i, ii, hostArr, websiteEndpoint;

  if(request.headers === undefined){
    return;
  }

  if(request.headers.host === undefined){
    return;
  }
  host = request.headers.host.split(':')[0];
  for(i = 0, ii = awsRegions.length; i < ii; i++) {
    endpoint = awsRegions[i].endpoint;
    websiteEndpoint = 's3-website-' + awsRegions[i].region + '.amazonaws.com';

    if(host !== endpoint && host.indexOf(endpoint) !== -1) {
      hostArr = host.split('.' + endpoint);
      hostname = hostArr[0];
      return hostname;
    }

    if(host !== endpoint && host.indexOf(websiteEndpoint) !== -1) {
      hostArr = host.split('.' + websiteEndpoint);
      hostname = hostArr[0];
      return hostname;
    }
  }

  return;
}

/**
* Get bucket name and/or object name from the path of a request
* @param {object} request - http request object
* @param {string} request - name of bucket obtained from host name
*/
utils.getNamesFromPath = function(request, nameFromHost) {
  let resources = {
    bucket: undefined,
    object: undefined
  };

  let path = url.parse(request.url).pathname

  if(nameFromHost !== undefined){
    resources.bucket = nameFromHost;

    resources.object = path;
  } else {
    let urlArr = path.split('/');
    if(urlArr.length > 1) {
      resources.bucket = urlArr[1];
      // only apply non-empty strings
      if(urlArr.slice(2).join("/")) {
        resources.object = urlArr.slice(2).join("/");
      }
    } else if (urlArr.length === 1){
      resources.bucket = urlArr[1];
    }
  }

  return resources;
}

/**
* Validate bucket name per naming rules and restrictions
* @param {string} bucketname - name of the bucket to be created
*/
utils.isValidBucketName = function(bucketname) {
  let ipAddressRegex, dnsCompatibleRegex;
  // Must be at least 3 and no more than 63 characters long.
  if(bucketname.length < 3 || bucketname.length > 63) {
    return false;
  }

  // Must not be an ip address
  ipAddressRegex = new RegExp(/(\d+\.){3}\d+/);
  if(bucketname.match(ipAddressRegex)) {
    return false;
  }

  // Must be dns compatible
  dnsCompatibleRegex = new RegExp(/^[a-z0-9]+([\.\-]{1}[a-z0-9]+)*$/);
  if(bucketname.match(dnsCompatibleRegex) === null) {
    return false;
  }

  return true;
}



utils.getContentMD5 = function(requestBody) {
  return crypto.createHash('md5').update(requestBody).digest('hex');
}


/**
* Pull user provided meta headers from request headers
* @param {object} headers - headers attached to the http request (lowercased)
* @return {object} all user meta headers
*/

utils.getMetaHeaders = function(headers) {
  let metaHeaders = {};
  for(let k in headers){
    if(k.substr(0, 11) === 'x-amz-meta-'){
      metaHeaders[k] = headers[k];
    }
  }
  return metaHeaders;
}

/**
* Create a unique key for either a bucket or an object
* @param {string} namespace - namespace of request
* @param {string} resource - either bucketname or bucketname + objectname
* @return {string} hash to use as bucket key or object key
*/

utils.getResourceUID = function(namespace, resource) {
  return crypto.createHash('md5').update(namespace + resource).digest('hex');
}


/**
* Modify http request object
* @param {object} request - http request object
* @return {object} request object with additional attributes
*/

utils.normalizeRequest = function(request) {
  request.lowerCaseHeaders = {};
  for(var key in request.headers){
    request.lowerCaseHeaders[key.toLowerCase()] = request.headers[key];
  }
  request.query = url.parse(request.url, true).query;
  //TODO: make the namespace come from a config variable.
  request.namespace = 'default';
  return request;
}


/**
* Add to http response headers
* @param {object} response - http response object
* @param {object} headers - key and value of new headers to add
* @return {object} response object with additional headers
*/

utils.buildResponseHeaders = function(response, headers) {

  for(let key in headers){
    response.setHeader(key, headers[key]);
  }

  // to be expanded in further implementation of logging of requests
  response.setHeader('x-amz-id-2', UUID.v4());
  response.setHeader('x-amz-request-id', UUID.v4());

  return response;
}

/**
* Modify response headers for an objectGet or objectHead request
* @param {object} headers - lowercased headers from request object
* @param {object} response - response object
* @param {object} responseMetaHeaders - object with additional headers to add to response object
* @return {object} response - modified response object
*/

 
utils.buildGetSuccessfulResponse = function(headers, response, responseMetaHeaders) {
  let additionalResponseHeaders = {};
  //TODO: If retrieved object is a delete marker, return x-amx-delete-marker header set to true. 
  if(headers['response-content-type']) {
    additionalResponseHeaders['Content-Type'] = headers['response-content-type']
  }
  if(headers['response-content-language']) {
    additionalResponseHeaders['Content-Language'] = headers['response-content-language']
  }
  if(headers['response-expires']) {
    additionalResponseHeaders['Expires'] = headers['response-expires']
  }
  if(headers['response-cache-control']) {
    additionalResponseHeaders['Cache-Control'] = headers['response-cache-control']
  }
  if(headers['response-content-disposition']) {
    additionalResponseHeaders['Content-Disposition'] = headers['response-content-disposition']
  }
  if(headers['response-content-encoding']) {
    additionalResponseHeaders['Content-Encoding'] = headers['response-content-encoding']
  }

  for(let k in responseMetaHeaders) {
    additionalResponseHeaders[k] = responseMetaHeaders[k];
  }
  response = utils.buildResponseHeaders(response, additionalResponseHeaders)
  response.writeHead(200);
   return response;
 };





module.exports = utils;
