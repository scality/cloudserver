'use strict';

const utils = require('../utils.js');
const services = require('../services.js');
const async = require('async');


/**
 * HEAD Object - Same as Get Object but only respond with headers (no actual body)
 * @param  {string} accessKey - user's access key
 * @param {object} metastore - metastore with buckets containing objects and their metadata
 * @param {object} request - normalized request object
 * @param {function} callback - callback to function in route
 * @return {function} callback with error and responseMetaHeaders as arguments
 *
 */

let objectHead = function (accessKey,  metastore, request, callback) {
    let bucketname = utils.getResourceNames(request).bucket;
    let bucketUID = utils.getResourceUID(request.namespace, bucketname);
    let objectKey = utils.getResourceNames(request).object;
    let objectUID = utils.getResourceUID(request.namespace, bucketname + objectKey);
    let metadataValParams = {accessKey: accessKey, bucketUID: bucketUID, objectKey: objectKey, metastore: metastore};
    let metadataCheckParams = {headers: request.lowerCaseHeaders};

    async.waterfall([
            function (next) {
                services.metadataValidateAuthorization(metadataValParams, next);
            },
            function (bucket, objectMetadata, next) {
                services.metadataChecks(bucket, objectMetadata, metadataCheckParams, next);
            }
    ], function (err, bucket, objectMetadata, responseMetaHeaders) {
        return callback(err, responseMetaHeaders);
    });
};

module.exports = objectHead;
