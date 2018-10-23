const { errors } = require('arsenal');

const abortMultipartUpload = require('./apiUtils/object/abortMultipartUpload');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const isLegacyAWSBehavior = require('../utilities/legacyAWSBehavior');
/**
 * multipartDelete - DELETE an open multipart upload from a bucket
 * @param  {AuthInfo} authInfo -Instance of AuthInfo class with requester's info
 * @param  {object} request - request object given by router,
 *                            includes normalized headers
 * @param  {object} log - the log request
 * @param  {function} callback - final callback to call with the
 *                          result and response headers
 * @return {undefined} calls callback from router
 * with err, result and responseMetaHeaders as arguments
 */
function multipartDelete(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'multipartDelete' });

    const bucketName = request.bucketName;
    const objectKey = request.objectKey;
    const uploadId = request.query.uploadId;

    abortMultipartUpload(authInfo, bucketName, objectKey, uploadId, log,
        (err, destinationBucket) => {
            const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, destinationBucket);
            const locationConstraint = destinationBucket ?
                destinationBucket.getLocationConstraint() : null;
            if (err === errors.NoSuchUpload) {
                log.trace('did not find valid mpu with uploadId' +
                `${uploadId}`, { method: 'multipartDelete' });
                // if legacy behavior is enabled for 'us-east-1' and
                // request is from 'us-east-1', return 404 instead of
                // 204
                if (isLegacyAWSBehavior(locationConstraint)) {
                    return callback(err, corsHeaders);
                }
                // otherwise ignore error and return 204 status code
                return callback(null, corsHeaders);
            }
            return callback(err, corsHeaders);
        });
}

module.exports = multipartDelete;
