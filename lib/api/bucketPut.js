var vaultGetResource = require("./services.js").vaultGetResource;
var xmlService = require('xml');
var utils = require('../utils.js');
var bucket = require('../bucket_mem.js');

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
  bucketname = utils.getResourceNames(request).bucket;
  if(bucketname === undefined) {
    return callback('Missing bucket name', null);
  }

  if(utils.isValidBucketName(bucketname) === false) {
    return callback('Bucket name is invalid', null);
  }

  var bucket = new Bucket();

	vaultRequest = { accessKey: accessKey, resource: "createBucket", bucketname: bucketname };
	vaultGetResource(vaultRequest, callback);
};

module.exports = bucketPut;
