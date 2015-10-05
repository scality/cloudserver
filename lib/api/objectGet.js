'use strict';

const utils = require('../utils.js');
const services = require('./services.js');
const async = require('async');


/**
 * GET Object - Get an object
 * @param  {string} accessKey - user's access key
 * @param {object} datastore - datastore with object data
 * @param {object} metastore - metastore with buckets containing objects and their metadata
 * @param {object} request - normalized request object
 * @param {function} callback - callback to function in route
 * @return {function} callback with error, object data result and responseMetaHeaders
 *
 */

let objectGet = function(accessKey, datastore,  metastore, request, callback){
  let bucketname = utils.getResourceNames(request).bucket;
  let bucketUID = utils.getResourceUID(request.namespace, bucketname);
  let objectKey = utils.getResourceNames(request).object;
  let objectUID = utils.getResourceUID(request.namespace, bucketname + objectKey);
  let metadataValParams = {accessKey: accessKey, bucketUID: bucketUID, objectUID: objectUID, metastore: metastore};
  let metadataCheckParams = {headers: request.lowerCaseHeaders};

async.waterfall([
      function(next){
        services.metadataValidateAuthorization(metadataValParams, next)
      },
      function(bucket, objectMetadata, next){
        services.metadataChecks(bucket, objectMetadata, metadataCheckParams, next);
      },
      function(bucket, objectMetadata, metaHeaders, next){
          services.getFromDatastore(bucket, objectMetadata, metaHeaders, datastore, next)
      }
  ], function (err, result, responseMetaHeaders) {
			return callback(err, result, responseMetaHeaders);
  });
};

module.exports = objectGet;