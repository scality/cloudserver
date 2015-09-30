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
    let contentMD5 = utils.getContentMD5(request.value);
    let metaHeaders = utils.getMetaHeaders(request.headers);
    let objectUID = utils.getResourceUID(request.namespace, bucketname + objectKey);
    let bucketUID = utils.getResourceUID(request.namespace, bucketname)
	let metadataValParams = {accessKey: accessKey, resource: "objectPut", bucketUID: bucketUID, objectUID: objectUID, metastore: metastore};
    let dataStoreParams = {value: request.body, resource: "objectPut", objectUID: objectUID};
    //Complete with items MD Store needs
    let metadataStoreParams = {ojectUID: objectUID};


    //Grab custom amz-meta- headers
    //Get resource names
    
	async.waterfall([
    services.metadataValidateAuthorization(metadataValParams, next),
    services.dataStore(bucket, objectExistsRes, datastore, dataStoreParams, next),
    services.metadataStore(bucket, objectExistsRes, location, metastore, metadataStoreParams, next)
    ], function (err, result) {
    	callback(err, result); //whatever we want to do here
    });

};


module.exports = objectPut;