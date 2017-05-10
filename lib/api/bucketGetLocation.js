const { errors } = require('arsenal');

const bucketShield = require('./apiUtils/bucket/bucketShield');
const { isBucketAuthorized } = require('./apiUtils/authorization/aclChecks');
const metadata = require('../metadata/wrapper');
const { pushMetric } = require('../utapi/utilities');
const escapeForXML = require('../utilities/escapeForXML');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');

const requestType = 'bucketOwnerAction';

/**
 * Bucket Get Location - Get bucket locationConstraint configuration
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */

function bucketGetLocation(authInfo, request, log, callback) {
    const bucketName = request.bucketName;
    const canonicalID = authInfo.getCanonicalID();

    return metadata.getBucket(bucketName, log, (err, bucket) => {
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
            log.debug('access denied for account on bucket', {
                requestType,
                method: 'bucketGetLocation',
            });
            return callback(errors.AccessDenied, null, corsHeaders);
        }

        let locationConstraint = bucket.getLocationConstraint();
        if (!locationConstraint || locationConstraint === 'us-east-1') {
          // AWS returns empty string if no region has been
          // provided or for us-east-1
          // Note: AWS JS SDK sends a request with locationConstraint us-east-1
          // if no locationConstraint provided.
            locationConstraint = '';
        }
        const xml = `<?xml version="1.0" encoding="UTF-8"?>
        <LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` +
            `${escapeForXML(locationConstraint)}</LocationConstraint>`;
        pushMetric('getBucketLocation', log, {
            authInfo,
            bucket: bucketName,
        });

        return callback(null, xml, corsHeaders);
    });
}

module.exports = bucketGetLocation;
