'use strict';

const utils = require('../utils.js');
const services = require('./services.js');
const vaultGetResource = services.vaultGetResource;
const dataConnect = services.dataConnect;
const metadataConnect = services.metadataConnect;



let _getmetadataHeaders = function(request){
	//NEED TO COMPLETE

};

/**
 * objectPut - Put object in a bucket
 * @param  {string} accessKey - user's accessKey
 * @param  {object} request - http request object
 * @param  {function} callback - callback to respond to http request with response code
 */

let objectPut = function(accessKey, request, callback) {
	let bucketname = utils.getResourceNames(request).bucket;
	let objectname = utils.getResourceNames(request).object;
	let requestInfo = {accessKey: accessKey, resource: "objectPut", bucketname: bucketname, objectname: objectname};
	vaultGetResource(requestInfo, function(responseCode){
		if(responseCode === 403 || responseCode === 404){
			return callback(responseCode);
		}
  
		dataConnect(requestInfo, request, function(err, dataResponse){
			if(err){
				return callback("Data Connection Error: " + err);
			}

			metadataConnect(dataResponse, metadata, function(err, metadataresponse){
				//if err
				//callback with err
				//otherwise
				//callback with etag

			});

		});

	});

};

module.exports = objectPut;