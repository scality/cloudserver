import { errors } from 'arsenal';

import { createBucket } from './apiUtils/bucket/bucketCreation';
import config from '../Config';
import aclUtils from '../utilities/aclUtils';
import { pushMetric } from '../utapi/utilities';

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
    const bucketName = request.bucketName;

    return createBucket(authInfo, bucketName, request.headers,
        locationConstraint, config.usEastBehavior, log, err => {
            if (err) {
                return callback(err);
            }
            pushMetric('createBucket', log, {
                bucket: bucketName,
            });
            return callback();
        });
}
