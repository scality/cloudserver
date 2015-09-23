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

  return hostname;
}

/**
* Get hostname from the path of a request
* @param {object} request - http request object
*/
utils.getBucketNameFromPath = function(request) {
  var hostname, urlArr;

  urlArr = request.url.split('/');
  if(urlArr[1] !== undefined) hostname = urlArr[1];

  return hostname;
}

module.exports = utils;
