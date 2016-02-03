import { parseString } from 'xml2js';

import services from '../services';

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
 * @param  {AuthInfo} Instance of AuthInfo class with requester's info
 * @param  {object} request - http request object
 * @param  {object} log - Werelogs logger
 * @param {function} callback - callback to server
 */
export default function bucketPut(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketPut' });

    if (authInfo.isRequesterPublicUser()) {
        log.warn('operation not available for public user');
        return callback('AccessDenied');
    }
    const bucketName = request.bucketName;

    let locationConstraint;
    if (request.post) {
        let xmlToParse = request.post;
        if (typeof xmlToParse === 'object') {
            xmlToParse = '<CreateBucketConfiguration xmlns='
                .concat(xmlToParse['<CreateBucketConfiguration xmlns']);
        }
        return parseString(xmlToParse, function parseStringCb(err, result) {
            if (err) {
                log.warn('request xml is malformed');
                return callback('MalformedXML');
            }
            if (!result.CreateBucketConfiguration
                || !result.CreateBucketConfiguration.LocationConstraint
                || !result.CreateBucketConfiguration.LocationConstraint[0]) {
                log.warn('request xml is malformed');
                return callback('MalformedXML');
            }
            locationConstraint =
                result.CreateBucketConfiguration.LocationConstraint[0];
            if (possibleLocations.indexOf(locationConstraint) < 0) {
                log.warn('invalid location constraint',
                    { locationConstraint });
                return callback('InvalidLocationConstraint');
            }
            log.trace('location constraint', { locationConstraint });

            services.createBucket(authInfo, bucketName,
                    request.lowerCaseHeaders, locationConstraint,
                    log, (err, success) => {
                        return callback(err, success);
                    });
        });
    }
    // TODO Check user policies to see if user is authorized
    // to create a bucket
    services.createBucket(authInfo, bucketName, request.lowerCaseHeaders,
        locationConstraint, log, (err, success) => {
            callback(err, success);
        });
}
