import { errors } from 'arsenal';

import bucketShield from './apiUtils/bucket/bucketShield';
import collectCorsHeaders from '../utilities/collectCorsHeaders';
import { convertToXml } from './apiUtils/bucket/bucketCors';
import { isBucketAuthorized } from './apiUtils/authorization/aclChecks';
import metadata from '../metadata/wrapper';
// import { pushMetric } from '../utapi/utilities';

const requestType = 'bucketOwnerAction';

/**
 * Bucket Get CORS - Get bucket cors configuration
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
export default function bucketGetCors(authInfo, request, log, callback) {
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
        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);

        if (!isBucketAuthorized(bucket, requestType, canonicalID)) {
            log.debug('access denied for user on bucket', {
                requestType,
                method: 'bucketGetCors',
            });
            return callback(errors.AccessDenied, null, corsHeaders);
        }

        const cors = bucket.getCors();
        if (!cors) {
            log.debug('cors configuration does not exist', {
                method: 'bucketGetCors',
            });
            return callback(errors.NoSuchCORSConfiguration, null, corsHeaders);
        }
        log.trace('converting cors configuration to xml');
        const xml = convertToXml(cors);

        // TODO: Add getBucketCors to Utapi Client action map
        // pushMetric('getBucketCors', log, { bucket: bucketName });
        return callback(null, xml, corsHeaders);
    });
}
