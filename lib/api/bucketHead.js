'use strict';

const utils = require('../utils.js');
const vaultGetResource = require("./services.js").vaultGetResource;

/**
 * bucketHead - Determine if bucket exists and if user has permission to access it
 * @param  {string} accessKey - user's accessKey
 * @param  {object} request - http request object
 * @param  {function} callback - callback to respond to http request with response code
 */

let bucketHead = function(accessKey, request, callback) {
	let bucketname = utils.getBucketName(request);
	let vaultRequest = {accessKey: accessKey, resource: "bucketHead", bucketname: bucketname};
	vaultGetResource(vaultRequest, function(responseCode){
		return callback(responseCode);
	});

};

module.exports = bucketHead;