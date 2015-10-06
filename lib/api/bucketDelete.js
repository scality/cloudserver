'use strict';

const utils = require('../utils.js');
const services = require('./services.js');
const async = require('async');

/**
 * bucketDelete - DELETE bucket (currently supports only non-versioned buckets)
 * @param  {string}   accessKey - user access key
 * @param  {object}   metastore - metadata storage endpoint
 * @param  {object}   request   - request object given by router, includes normalized headers
 * @param  {function} callback  - final callback to call with the result and response headers
 */
let bucketDelete = function(accessKey,  metastore, request, callback) {

  let resourceRes = utils.getResourceNames(request)
  let bucketname = resourceRes.bucket;
  let bucketUID = utils.getResourceUID(request.namespace, bucketname);
  let metadataValParams = {
    accessKey: accessKey,
    bucketUID: bucketUID,
    metastore: metastore
  };
  let metadataCheckParams = {
    headers: request.lowerCaseHeaders
  };

  async.waterfall([
      function(next){
        services.metadataValidateAuthorization(metadataValParams, next)
      },
      function(bucket, objectMetadata, next){
        services.bucketMetadataChecks(bucket, metadataCheckParams, next)
      },
      function(bucket, responseMetaHeaders, next){
          services.deleteBucket(bucket, responseMetaHeaders, metastore, bucketUID, next)
      }
  ], function (err, result, responseMetaHeaders) {
      return callback(err, result, responseMetaHeaders);
  });
}

module.exports = bucketDelete;
