import { parseString } from 'xml2js';

import services from '../services';
import utils from '../utils';

// These are the valid values for S3.
// Could be configured differently for ironman
const possibleLocations = [
    'us-east-1',
    'us-west-1',
    'us-west-2',
    'eu-west-1',
    'eu-central-1',
    'ap-southeast-1',
    'ap-northeast-1',
    'ap-southeast-2',
    'sa-east-1',
];

/*
   Format of xml request:

   <?xml version="1.0" encoding="UTF-8"?>
   <CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <LocationConstraint>us-west-1</LocationConstraint>
   </CreateBucketConfiguration>
   */

/**
 * PUT Service - Create bucket for the user
 * @param  {string} accessKey - user's access key
 * @param  {object} metastore - in memory metadata store
 * @param  {object} request - http request object
 * @param  {function} log - Werelogs logger
 * @param {function} callback - callback to server
 */
export default function bucketPut(accessKey, metastore, request, log,
    callback) {
    if (accessKey === 'http://acs.amazonaws.com/groups/global/AllUsers') {
        return callback('AccessDenied');
    }
    const bucketName = utils.getResourceNames(request).bucket;
    if (utils.isValidBucketName(bucketName) === false) {
        return callback('InvalidBucketName');
    }

    let locationConstraint;
    if (request.post) {
        let xmlToParse = request.post;
        if (typeof xmlToParse === 'object') {
            xmlToParse = '<CreateBucketConfiguration xmlns='
                .concat(xmlToParse['<CreateBucketConfiguration xmlns']);
        }
        return parseString(xmlToParse, function parseStringCb(err, result) {
            if (err) {
                return callback('MalformedXML');
            }
            if (!result.CreateBucketConfiguration
                || !result.CreateBucketConfiguration.LocationConstraint
                || !result.CreateBucketConfiguration.LocationConstraint[0]) {
                return callback('MalformedXML');
            }
            locationConstraint =
                result.CreateBucketConfiguration.LocationConstraint[0];

            if (possibleLocations.indexOf(locationConstraint) < 0) {
                return callback('InvalidLocationConstraint');
            }

            services.createBucket(accessKey, bucketName,
                    request.lowerCaseHeaders, locationConstraint, metastore,
                    log, (err, success) => {
                        return callback(err, success);
                    });
        });
    }
    // TODO Check user policies to see if user is authorized
    // to create a bucket
    services.createBucket(accessKey, bucketName, request.lowerCaseHeaders,
        locationConstraint, metastore, log, (err, success) => {
            callback(err, success);
        });
}
