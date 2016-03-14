import { errors } from 'arsenal';
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
    log.debug('processing request', { method: 'bucketDelete' });

    if (authInfo.isRequesterPublicUser()) {
        log.warn('operation not available for public user');
        return cb(errors.AccessDenied);
    }
    const bucketName = request.bucketName;

    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: 'bucketDelete',
        log,
    };

    services.metadataValidateAuthorization(metadataValParams, err => {
        if (err) {
            log.warn('error processing request',
                { method: 'metadataValidateAuthorization', error: err });
            return cb(err);
        }
        log.trace('passed checks', { method: 'metadataValidateAuthorization' });
        return services.deleteBucket(bucketName, authInfo.getCanonicalID(),
                                     log, cb);
    });
    return;
}
