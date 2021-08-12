const { errors } = require('arsenal');

const abortMultipartUpload = require('./apiUtils/object/abortMultipartUpload');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const isLegacyAWSBehavior = require('../utilities/legacyAWSBehavior');
const monitoring = require('../utilities/monitoringHandler');
const { pushMetric } = require('../utapi/utilities');

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
        (err, destinationBucket, partSizeSum) => {
            const corsHeaders = collectCorsHeaders(request.headers.origin,
                request.method, destinationBucket);
            const location = destinationBucket ?
                destinationBucket.getLocationConstraint() : null;
            if (err && err !== errors.NoSuchUpload) {
                return callback(err, corsHeaders);
            }
            if (err === errors.NoSuchUpload && isLegacyAWSBehavior(location)) {
                log.trace('did not find valid mpu with uploadId', {
                    method: 'multipartDelete',
                    uploadId,
                });
                monitoring.promMetrics('DELETE', bucketName, 400,
                    'abortMultipartUpload');
                return callback(err, corsHeaders);
            }
            monitoring.promMetrics('DELETE', bucketName, 400,
                'abortMultipartUpload');
            if (!err) {
                pushMetric('abortMultipartUpload', log, {
                    authInfo,
                    canonicalID: destinationBucket.getOwner(),
                    bucket: bucketName,
                    keys: [objectKey],
                    byteLength: partSizeSum,
                    location,
                });

                log.addDefaultFields({
                    bytesDeleted: partSizeSum,
                });
            }
            return callback(null, corsHeaders);
        }, request);
}

module.exports = multipartDelete;
