import { errors } from 'arsenal';
import { parseString } from 'xml2js';

import utils from '../utils';
import services from '../services';

const possibleLocations = utils.getAllRegions();

/*
   Format of xml request:

   <?xml version="1.0" encoding="UTF-8"?>
   <CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <LocationConstraint>us-west-1</LocationConstraint>
   </CreateBucketConfiguration>
   */

/**
 * PUT Service - Create bucket for the user
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
export default function bucketPut(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketPut' });

    if (authInfo.isRequesterPublicUser()) {
        log.debug('operation not available for public user');
        return callback(errors.AccessDenied);
    }
    const bucketName = request.bucketName;

    let locationConstraint;
    if (request.post) {
        let xmlToParse = request.post;
        if (typeof xmlToParse === 'object') {
            xmlToParse = '<CreateBucketConfiguration xmlns='
                .concat(xmlToParse['<CreateBucketConfiguration xmlns']);
        }
        return parseString(xmlToParse, (err, result) => {
            if (err) {
                log.debug('request xml is malformed');
                return callback(errors.MalformedXML);
            }
            if (!result.CreateBucketConfiguration
                || !result.CreateBucketConfiguration.LocationConstraint
                || !result.CreateBucketConfiguration.LocationConstraint[0]) {
                log.debug('request xml is malformed');
                return callback(errors.MalformedXML);
            }
            locationConstraint =
                result.CreateBucketConfiguration.LocationConstraint[0];
            if (possibleLocations.indexOf(locationConstraint) < 0) {
                log.debug('invalid location constraint',
                    { locationConstraint });
                return callback(errors.InvalidLocationConstraint);
            }
            log.trace('location constraint', { locationConstraint });

            return services.createBucket(authInfo, bucketName, request.headers,
                                         locationConstraint, log, callback);
        });
    }
    // TODO Check user policies to see if user is authorized
    // to create a bucket
    return services.createBucket(authInfo, bucketName, request.headers,
                                 locationConstraint, log, callback);
}
