import { errors } from 'arsenal';

import bucketShield from './apiUtils/bucket/bucketShield';
import collectCorsHeaders from '../utilities/collectCorsHeaders';
import { isBucketAuthorized } from './apiUtils/authorization/aclChecks';
import metadata from '../metadata/wrapper';
import { pushMetric } from '../utapi/utilities';

const requestType = 'bucketOwnerAction';

/**
 * Bucket Delete CORS - Delete bucket cors configuration
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
export default function bucketDeleteCors(authInfo, request, log, callback) {
    const bucketName = request.bucketName;
    const canonicalID = authInfo.getCanonicalID();

    return metadata.getBucket(bucketName, log, (err, bucket) => {
        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (err) {
            log.debug('metadata getbucket failed', { error: err });
            return callback(err);
        }
        if (bucketShield(bucket, requestType)) {
            return callback(errors.NoSuchBucket);
        }
        log.trace('found bucket in metadata');

        if (!isBucketAuthorized(bucket, requestType, canonicalID)) {
            log.debug('access denied for user on bucket', {
                requestType,
                method: 'bucketDeleteCors',
            });
            return callback(errors.AccessDenied, corsHeaders);
        }

        const cors = bucket.getCors();
        if (!cors) {
            log.trace('no existing cors configuration', {
                method: 'bucketDeleteCors',
            });
            pushMetric('deleteBucketCors', log, {
                authInfo,
                bucket: bucketName,
            });
            return callback(null, corsHeaders);
        }

        log.trace('deleting cors configuration in metadata');
        bucket.setCors(null);
        return metadata.updateBucket(bucketName, bucket, log, err => {
            if (err) {
                return callback(err, corsHeaders);
            }
            pushMetric('deleteBucketCors', log, {
                authInfo,
                bucket: bucketName,
            });
            return callback(err, corsHeaders);
        });
    });
}
