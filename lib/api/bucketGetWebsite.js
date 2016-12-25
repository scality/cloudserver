import { errors } from 'arsenal';

import bucketShield from './apiUtils/bucket/bucketShield';
import { convertToXml } from './apiUtils/bucket/bucketWebsite';
import { isBucketAuthorized } from './apiUtils/authorization/aclChecks';
import metadata from '../metadata/wrapper';
import { pushMetric } from '../utapi/utilities';

const requestType = 'bucketOwnerAction';

/**
 * Bucket Get Website - Get bucket website configuration
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
export default function bucketGetWebsite(authInfo, request, log, callback) {
    const bucketName = request.bucketName;
    const canonicalID = authInfo.getCanonicalID();

    metadata.getBucket(bucketName, log, (err, bucket) => {
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
                method: 'bucketGetWebsite',
            });
            return callback(errors.AccessDenied);
        }

        const websiteConfig = bucket.getWebsiteConfiguration();
        if (!websiteConfig) {
            log.debug('bucket website configuration does not exist', {
                method: 'bucketGetWebsite',
            });
            return callback(errors.NoSuchWebsiteConfiguration);
        }
        log.trace('converting website configuration to xml');
        const xml = convertToXml(websiteConfig);

        pushMetric('getBucketWebsite', log, { bucket: bucketName });
        return callback(null, xml);
    });
}
