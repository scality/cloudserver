var vaultGetResource = require("./services.js").vaultGetResource;
var xmlService = require('xml');
var utils = require('../utils.js');

/*
	Format of xml response:

	<?xml version="1.0" encoding="UTF-8"?>
  <CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <LocationConstraint>us-west-1</LocationConstraint>
  </CreateBucketConfiguration>
*/

/**
 * PUT Service - Create bucket for the user
 * @param  {object} request - http request object
 * @param  {object} response - http response object
 */

var bucketPut = function(accessKey, request, callback){
  var bucketname, vaultRequest;
  bucketname = utils.getBucketName(request);
  if(bucketname === undefined) {
    return callback('Missing bucket name', null);
  }

  if(utils.isValidBucketName(bucketname) === false) {
    return callback('Bucket name is invalid', null);
  }

	vaultRequest = { accessKey: accessKey, resource: "createBucket", bucketname: bucketname };

	vaultGetResource(vaultRequest, callback);
};

module.exports = getBucketsbyUser;
