import { errors } from 'arsenal';

import { createBucket } from './apiUtils/bucket/bucketCreation';
import collectCorsHeaders from '../utilities/collectCorsHeaders';
import config from '../Config';
import aclUtils from '../utilities/aclUtils';
import { pushMetric } from '../utapi/utilities';

let locationConstraintChecked;

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
 * @param {string | undefined} locationConstraint - locationConstraint for
 * bucket (if any)
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
export default function bucketPut(authInfo, request, locationConstraint, log,
    callback) {
    log.debug('processing request', { method: 'bucketPut' });

    if (authInfo.isRequesterPublicUser()) {
        log.debug('operation not available for public user');
        return callback(errors.AccessDenied);
    }
    if (!aclUtils.checkGrantHeaderValidity(request.headers)) {
        log.trace('invalid acl header');
        return callback(errors.InvalidArgument);
    }
    // - AWS JS SDK sends a request with locationConstraint us-east-1
    // if no locationConstraint provided.
    if (locationConstraint && Object.keys(config.locationConstraints).
        indexOf(locationConstraint) < 0) {
        log.trace('locationConstraint is invalid',
          { locationConstraint });
        return callback(errors.InvalidLocationConstraint);
    }

    if (!locationConstraint && request.parsedHost &&
      config.restEndpoints[request.parsedHost]) {
        locationConstraintChecked = config.restEndpoints[request.parsedHost];
    } else {
        locationConstraintChecked = locationConstraint;
    }
    const bucketName = request.bucketName;

    return createBucket(authInfo, bucketName, request.headers,
        locationConstraintChecked, log,
        (err, previousBucket) => {
            // if bucket already existed, gather any relevant cors headers
            const corsHeaders = collectCorsHeaders(request.headers.origin,
                request.method, previousBucket);
            if (err) {
                return callback(err, corsHeaders);
            }
            pushMetric('createBucket', log, {
                authInfo,
                bucket: bucketName,
            });
            return callback(null, corsHeaders);
        });
}
