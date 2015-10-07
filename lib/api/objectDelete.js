'use strict';

const utils = require('../utils.js');
const services = require('../services.js');
const async = require('async');

/**
 * objectDelete - DELETE an object from a bucket (currently supports only non-versioned buckets)
 * @param  {string}   accessKey - user access key
 * @param  {object}   datastore - data storage endpoint
 * @param  {object}   metastore - metadata storage endpoint
 * @param  {object}   request   - request object given by router, includes normalized headers
 * @param  {function} callback  - final callback to call with the result and response headers
 */
let objectDelete = function(accessKey, datastore,  metastore, request, callback) {
  let resourceRes = utils.getResourceNames(request)
  let bucketname = resourceRes.bucket;
  let bucketUID = utils.getResourceUID(request.namespace, bucketname);
  let objectKey = resourceRes.object;
  let objectUID = utils.getResourceUID(request.namespace, bucketname + objectKey);
  let metadataValParams = {
    accessKey: accessKey,
    bucketUID: bucketUID,
    objectUID: objectUID,
    metastore: metastore,
    objectKey: objectKey
  };
  let metadataCheckParams = {
    headers: request.lowerCaseHeaders
  };
  async.waterfall([
      function(next){
        services.metadataValidateAuthorization(metadataValParams, next)
      },
      function(bucket, objectMetadata, next){
        services.metadataChecks(bucket, objectMetadata, metadataCheckParams, next);
      },
      function(bucket, objectMetadata, metaHeaders, next){
          services.deleteObjectFromBucket(bucket, objectMetadata, metaHeaders, objectUID, next)
      }
  ], function (err, result, responseMetaHeaders) {
      return callback(err, result, responseMetaHeaders);
  });
}

module.exports = objectDelete;
