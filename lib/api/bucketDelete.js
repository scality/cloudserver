import services from '../services';

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
export default function bucketDelete(authInfo, request, log, cb) {
    log.debug('Processing the request in Bucket DELETE api');
    if (authInfo.isRequesterPublicUser()) {
        log.error('Access Denied: Operation not available for AllUsers group');
        return cb('AccessDenied');
    }
    const bucketName = request.bucketName;
    log.debug(`Bucket Name: ${bucketName}`);
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: 'bucketDelete',
        log,
    };

    services.metadataValidateAuthorization(metadataValParams, err => {
        if (err) {
            log.error(`Error from metadata validate authorization checks: ` +
                `${err}`);
            return cb(err);
        }
        log.trace('Passed metadata validate authorization checks');
        services.deleteBucket(bucketName, authInfo.getCanonicalID(), log, cb);
    });
}
