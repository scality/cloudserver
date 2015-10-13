'use strict';

const parseString = require('xml2js').parseString;
const utils = require('../utils.js');
const services = require('../services.js');
// These are the valid values for S3.  Could be configured differently for ironman
const possibleLocations = [
    'us-west-1',
    'us-west-2',
    'eu-west-1',
    'eu-central-1',
    'ap-southeast-1',
    'ap-northeast-1',
    'ap-southeast-2',
    'sa-east-1'];

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

let bucketPut = function (accessKey, metastore, request, callback) {
    let bucketname = utils.getResourceNames(request).bucket;

    if (utils.isValidBucketName(bucketname) === false) {
        return callback('InvalidBucketName');
    }

    let bucketUID = utils.getResourceUID(request.namespace, bucketname);
    let locationConstraint;
    if (request.post) {
        return parseString(request.post, function (err, result) {
            if (err) {
                return callback('MalformedXML');
            }
            if (!result['CreateBucketConfiguration'] ||
                    !result['CreateBucketConfiguration']['LocationConstraint'] ||
                    !result['CreateBucketConfiguration']['LocationConstraint'][0]) {
                return callback('MalformedXML');
            }

            locationConstraint = result['CreateBucketConfiguration']['LocationConstraint'][0];

            if (possibleLocations.indexOf(locationConstraint) < 0) {
          return callback('InvalidLocationConstraint');
      }

            services.createBucket(accessKey, bucketname, bucketUID, request.lowerCaseHeaders, locationConstraint, metastore, function (err, success) {
                return callback(err, success);
            });
        });
    }

    services.createBucket(accessKey, bucketname, bucketUID, request.lowerCaseHeaders, locationConstraint, metastore, function (err, success) {
        callback(err, success);
    });

};

module.exports = bucketPut;
