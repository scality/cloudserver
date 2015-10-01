'use strict';

const parseString = require('xml2js').parseString;
const utils = require('../utils.js');
const services = require('./services.js');

// const bucket = require('../bucket_mem.js');

/*
	Format of xml request:

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

let bucketPut = function(accessKey, metastore, request, callback){
  let bucketname = utils.getResourceNames(request).bucket;
  if(bucketname === undefined) {
    return callback('Missing bucket name', null);
  }

  if(utils.isValidBucketName(bucketname) === false) {
    return callback('Bucket name is invalid', null);
  }

  let bucketUID = utils.getResourceUID(request.namespace, bucketname);
  let locationConstraint;
  if(request.post) {
    parseString(request.post, function (err, result) {
      if(err) {
        return callback('Improper XML', result);
      }
      let xml = JSON.stringify(result);
      locationConstraint = xml.CreateBucketConfiguration.LocationConstraint;
    });
  }

  services.metadataStoreBucket(accessKey, bucketUID, request.lowerCaseHeaders, locationConstraint, metastore, function(err, result) {
    callback(err, result);
  })

  //check whether user policy allows user to create buckets
  //check with metastore whether bucket exists,
  //if exists, error
  //if not, create bucket in metadata store with metadata -- locationconstraint from xml, acl headers, etc.


 //  var bucket = new Bucket();

	// vaultRequest = { accessKey: accessKey, resource: "createBucket", bucketname: bucketname };
	// vaultGetResource(vaultRequest, callback);
};

module.exports = bucketPut;