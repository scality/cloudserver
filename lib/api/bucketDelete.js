const { errors } = require('arsenal');

const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const deleteBucket = require('./apiUtils/bucket/bucketDeletion');
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
        monitoring.promMetrics(
            'DELETE', request.bucketName, 403, 'deleteBucket');
        return cb(errors.AccessDenied);
    }
    const bucketName = request.bucketName;

    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: 'bucketDelete',
        request,
    };

    return metadataValidateBucket(metadataValParams, log,
        (err, bucketMD) => {
            const corsHeaders = collectCorsHeaders(request.headers.origin,
                request.method, bucketMD);
            if (err) {
                log.debug('error processing request',
                    { method: 'metadataValidateBucket', error: err });
                monitoring.promMetrics(
                    'DELETE', bucketName, err.code, 'deleteBucket');
                return cb(err, corsHeaders);
            }
            log.trace('passed checks',
                { method: 'metadataValidateBucket' });
            return deleteBucket(authInfo, bucketMD, bucketName,
                authInfo.getCanonicalID(), log, err => {
                    if (err) {
                        monitoring.promMetrics(
                            'DELETE', bucketName, err.code, 'deleteBucket');
                        return cb(err, corsHeaders);
                    }
                    pushMetric('deleteBucket', log, {
                        authInfo,
                        bucket: bucketName,
                    });
                    monitoring.promMetrics(
                        'DELETE', bucketName, '204', 'deleteBucket');
                    return cb(null, corsHeaders);
                });
        });
}

module.exports = bucketDelete;
