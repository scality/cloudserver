'use strict';

const utils = require('../utils.js');
const services = require('../services.js');

/**
 * bucketHead - Determine if bucket exists and if user has permission to access it
 * @param  {string} accessKey - user's accessKey
 * @param {object} metastore - metadata store
 * @param  {object} request - http request object
 * @param  {function} callback - callback to respond to http request with either error code or success
 */

let bucketHead = function(accessKey, metastore, request, callback) {
	let bucketname = utils.getResourceNames(request).bucket;
	let bucketUID = utils.getResourceUID(request.namespace, bucketname);
	let metadataValParams = {accessKey: accessKey, bucketUID: bucketUID, metastore: metastore};


	services.metadataValidateAuthorization(metadataValParams, function (err, bucket) {
		return callback(err, "Bucket exists and user authorized -- 200");
	});
};

module.exports = bucketHead;
