import { errors } from 'arsenal';
import async from 'async';

import bucketShield from './apiUtils/bucket/bucketShield';
import { isBucketAuthorized } from './apiUtils/authorization/aclChecks';
import metadata from '../metadata/wrapper';
import { parseWebsiteConfigXml } from './apiUtils/bucket/bucketWebsite';

/**
 * Bucket Put Website - Create bucket website configuration
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
export default function bucketPutWebsite(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketPutWebsite' });
    const bucketName = request.bucketName;
    const requestType = 'bucketPutWebsite';
    const canonicalID = authInfo.getCanonicalID();

    if (!request.post) {
        return callback(errors.MissingRequestBodyError);
    }
    return async.waterfall([
        function parseXmlBody(next) {
            log.trace('parsing website configuration');
            return parseWebsiteConfigXml(request.post, log, next);
        },
        function getBucketfromMetadata(config, next) {
            metadata.getBucket(bucketName, log, (err, bucket) => {
                if (err) {
                    log.debug('metadata getbucket failed', { error: err });
                    return next(err);
                }
                if (bucketShield(bucket, requestType)) {
                    return next(errors.NoSuchBucket);
                }
                log.trace('found bucket in metadata');
                return next(null, bucket, config);
            });
        },
        function validateBucketAuthorization(bucket, config, next) {
            if (!isBucketAuthorized(bucket, requestType, canonicalID)) {
                log.debug('access denied for user on bucket', {
                    requestType,
                });
                return next(errors.AccessDenied);
            }
            return next(null, bucket, config);
        },
        function updateBucketMetadata(bucket, config, next) {
            log.trace('updating bucket website configuration in metadata');
            bucket.setWebsiteConfiguration(config);
            metadata.updateBucket(bucketName, bucket, log, next);
        },
    ], err => {
        if (err) {
            log.trace('error processing request', { error: err,
                method: 'bucketPutWebsite' });
        }
        return callback(err);
    });
}
