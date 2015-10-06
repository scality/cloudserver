'use strict';

const utils = require('../utils.js');
const services = require('./services.js');
const async = require('async');

/**
 * bucketGet - Return list of objects in bucket
 * @param  {string} accessKey - user's accessKey
 * @param {object} metastore - metadata store
 * @param  {object} request - http request object
 * @param  {function} callback - callback to respond to http request with either error code or success
 */

let bucketGet = function(accessKey, metastore, request, callback) {
	let params = request.query;
	let bucketname = utils.getResourceNames(request).bucket;
	let bucketUID = utils.getResourceUID(request.namespace, bucketname);
	let metadataValParams = {accessKey: accessKey, bucketUID: bucketUID, metastore: metastore};
	let listingParams = {
		delimiter: params.delimeter, 
		marker: params.marker, 
		maxKeys: parameter.maxKeys, 
		prefix: paramater.prefix}

	async.waterfall([
	      function(next){
	        services.metadataValidateAuthorization(metadataValParams, next)
	      },
	       //CONTINUE HERE (call bucket_mem.js listing function with listingParams and convert response to xml)
	      function(bucket, next){
	        services.getObjectListing(bucket, listingParams, next);
	      }
	  ], function (err, list) {
				return callback(err, list);
	  });
	};


module.exports = bucketGet;