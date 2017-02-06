import { errors } from 'arsenal';

import bucketShield from './apiUtils/bucket/bucketShield';
import collectCorsHeaders from '../utilities/collectCorsHeaders';
import { isBucketAuthorized } from './apiUtils/authorization/aclChecks';
import metadata from '../metadata/wrapper';
import { pushMetric } from '../utapi/utilities';

const requestType = 'bucketOwnerAction';

export default function bucketDeleteWebsite(authInfo, request, log, callback) {
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
                method: 'bucketDeleteWebsite',
            });
            return callback(errors.AccessDenied, corsHeaders);
        }

        const websiteConfig = bucket.getWebsiteConfiguration();
        if (!websiteConfig) {
            log.trace('no existing website configuration', {
                method: 'bucketDeleteWebsite',
            });
            pushMetric('deleteBucketWebsite', log, {
                authInfo,
                bucket: bucketName,
            });
            return callback(null, corsHeaders);
        }

        log.trace('deleting website configuration in metadata');
        bucket.setWebsiteConfiguration(null);
        return metadata.updateBucket(bucketName, bucket, log, err => {
            if (err) {
                return callback(err, corsHeaders);
            }
            pushMetric('deleteBucketWebsite', log, {
                authInfo,
                bucket: bucketName,
            });
            return callback(null, corsHeaders);
        });
    });
}
