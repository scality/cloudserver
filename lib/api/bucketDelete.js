const { errors } = require('arsenal');

const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const { deleteBucket } = require('./apiUtils/bucket/bucketDeletion');
const { metadataValidateBucket } = require('../metadata/metadataUtils');
const { pushMetric } = require('../utapi/utilities');
const monitoring = require('../utilities/monitoringHandler');

/**
 * bucketDelete - DELETE bucket (currently supports only non-versioned buckets)
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - request object given by router
 *                           including normalized headers
 * @param {function} log - Werelogs log service
 * @param {function} cb - final callback to call
 *                        with the result and response headers
 * @return {undefined}
 */
function bucketDelete(authInfo, request, log, cb) {
    log.debug('processing request', { method: 'bucketDelete' });

    if (authInfo.isRequesterPublicUser()) {
        log.debug('operation not available for public user');
        return cb(errors.AccessDenied);
    }
    const bucketName = request.bucketName;

    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: 'bucketDelete',
    };

    return metadataValidateBucket(metadataValParams, log,
        (err, bucketMD) => {
            const corsHeaders = collectCorsHeaders(request.headers.origin,
                request.method, bucketMD);
            if (err) {
                log.debug('error processing request',
                    { method: 'metadataValidateBucket', error: err });
                return cb(err, corsHeaders);
            }
            log.trace('passed checks',
                { method: 'metadataValidateBucket' });
            return deleteBucket(bucketMD, bucketName, authInfo.getCanonicalID(),
                log, err => {
                    if (err) {
                        return cb(err, corsHeaders);
                    }
                    pushMetric('deleteBucket', log, {
                        authInfo,
                        bucket: bucketName,
                    });
                    monitoring.currentBucketNumber.dec();
                    return cb(null, corsHeaders);
                });
        });
}

module.exports = bucketDelete;
