'use strict';

const parseString = require('xml2js').parseString;
const utils = require('../utils.js');
const services = require('../services.js');

/*
	Format of xml request:

	<?xml version="1.0" encoding="UTF-8"?>
  <CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <LocationConstraint>us-west-1</LocationConstraint>
  </CreateBucketConfiguration>
*/

/**
 * PUT Service - Create bucket for the user
 * @param  {string} accessKey - user's access key
 * @param  {object} metastore - in memory metadata store
 * @param  {object} request - http request object
 * @param {function} callback - callback to server
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
      locationConstraint = result['CreateBucketConfiguration']['LocationConstraint'][0];
    });
  }

  services.createBucket(accessKey, bucketname, bucketUID, request.lowerCaseHeaders, locationConstraint, metastore, function(err, result) {
    callback(err, result);
  })

};

module.exports = bucketPut;
