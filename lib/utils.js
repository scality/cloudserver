var parseXmlString = require('xml2js').parseString;

var utils = {};

var awsRegions = [
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
  var host, endpoint, region, i, ii, hostArr;
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
* Get hostname from the request
* @param {object} request - http request object
*/
utils.getBucketName = function(request) {
  return this.getBucketNameFromHost(request) || this.getBucketNameFromPath(request);
}

/**
* Get hostname from the request of a virtually hosted bucket
* @param {object} request - http request object
*/
utils.getBucketNameFromHost = function(request) {
  var host, hostname, endpoint, region, i, ii, hostArr;

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
* Get hostname from the path of a request
* @param {object} request - http request object
*/
utils.getBucketNameFromPath = function(request) {
  var hostname, urlArr;

  urlArr = request.url.split('/');
  if(urlArr[1] !== undefined) {
    hostname = urlArr[1];
  }

  return hostname;
}

/**
* Validate bucket name per naming rules and restrictions
* @param {string} bucketname - name of the bucket to be created
*/
utils.isValidBucketName = function(bucketname) {
  var ipAddressRegex, dnsCompatibleRegex;
  // Must be at least 3 and no more than 63 characters long.
  if(bucketname.length < 3 || bucketname.length > 63) {
    return false;
  }

  // Must not an ip address
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

module.exports = utils;
