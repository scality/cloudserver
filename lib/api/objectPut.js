'use strict';

const utils = require('../utils.js');
const services = require('./services.js');
const vaultGetResource = services.vaultGetResource;
const dataConnect = services.dataConnect;
const metadataConnect = services.metadataConnect;
const async = require('async');


let objectPut = function(accessKey, datastore, metastore, request, callback) {
	let bucketname = utils.getResourceNames(request).bucket;
	let objectKey = utils.getResourceNames(request).object;
	let metadataRequest = {accessKey: accessKey, resource: "objectPut", bucketname: bucketname, objectKey: objectKey};
	let dataRequest = {};

	async.waterfall([
    services.metadataValidate(metadataRequest, next),
    services.dataStore(objectExistsRes, datastore, dataRequest, next),
    services.metadataStore(objectExistsRes, location, metastore, metadataStoreRequest, next)
    ], function (err, result) {
    	callback(err, result); //whatever we want to do here
    });

};


module.exports = objectPut;