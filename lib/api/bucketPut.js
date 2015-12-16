import { parseString } from 'xml2js';

import utils from '../utils';
import services from '../services';

// These are the valid values for S3.
// Could be configured differently for ironman
const possibleLocations = [
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
 * @param {function} callback - callback to server
 */
export default function bucketPut(accessKey, metastore, request, callback) {
    const bucketname = utils.getResourceNames(request).bucket;
    if (utils.isValidBucketName(bucketname) === false) {
        return callback('InvalidBucketName');
    }

    const bucketUID = utils.getResourceUID(request.namespace, bucketname);
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
            if (!result.CreateBucketConfiguration ||
                    !result.CreateBucketConfiguration.LocationConstraint ||
                    !result.CreateBucketConfiguration.LocationConstraint[0]) {
                return callback('MalformedXML');
            }
            locationConstraint =
                result.CreateBucketConfiguration.LocationConstraint[0];

            if (possibleLocations.indexOf(locationConstraint) < 0) {
                return callback('InvalidLocationConstraint');
            }

            services.createBucket(accessKey, bucketname, bucketUID,
                    request.lowerCaseHeaders, locationConstraint,
                    metastore, (err, success) => {
                        return callback(err, success);
                    });
        });
    }
    // TODO Check user policies to see if user is authorized
    // to create a bucket
    services.createBucket(accessKey, bucketname, bucketUID,
        request.lowerCaseHeaders, locationConstraint,
        metastore, (err, success) => {
            callback(err, success);
        });
}
