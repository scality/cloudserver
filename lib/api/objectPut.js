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
    let contentMD5 = "test";
    // let contentMD5 = utils.getContentMD5(request.body);
    let metaHeaders = utils.getMetaHeaders(request.lowerCaseHeaders);
    let objectUID = utils.getResourceUID(request.namespace, bucketname + objectKey);
    let bucketUID = utils.getResourceUID(request.namespace, bucketname)
	let metadataValParams = {accessKey: accessKey, bucketUID: bucketUID, objectUID: objectUID, metastore: metastore};
    let dataStoreParams = {contentMD5: contentMD5, headers: request.lowerCaseHeaders, value: request.body, objectUID: objectUID};
    let metadataStoreParams = {objectUID: objectUID, metaHeaders: metaHeaders, headers: request.lowerCaseHeaders, contentMD5: contentMD5};
    
	async.waterfall([
        function(next){
            services.metadataValidateAuthorization(metadataValParams, next)
        },
        function(bucket, objectExistsRes, next){
            services.dataStore(bucket, objectExistsRes, datastore, dataStoreParams, next)
        },
        function(bucket, objectExistsRes, location, next){
            services.metadataStore(bucket, objectExistsRes, location, metastore, metadataStoreParams, next)
        }
    ], function (err, result) {
        console.log("err in callback", err);
    	callback(err, result); 
    });

};


module.exports = objectPut;