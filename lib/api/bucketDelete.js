import { errors } from 'arsenal';

import { deleteBucket } from './apiUtils/bucket/bucketDeletion';
import services from '../services';
import { pushMetric } from '../utapi/utilities';

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
        log.debug('operation not available for public user');
        return cb(errors.AccessDenied);
    }
    const bucketName = request.bucketName;

    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: 'bucketDelete',
        log,
    };

    return services.metadataValidateAuthorization(metadataValParams,
        (err, bucketMD) => {
            if (err) {
                log.debug('error processing request',
                    { method: 'metadataValidateAuthorization', error: err });
                return cb(err);
            }
            log.trace('passed checks',
                { method: 'metadataValidateAuthorization' });
            return deleteBucket(bucketMD, bucketName, authInfo.getCanonicalID(),
                log, err => {
                    if (err) {
                        return cb(err);
                    }
                    pushMetric('deleteBucket', log, {
                        authInfo,
                        bucket: bucketName,
                    });
                    return cb();
                });
        });
}
