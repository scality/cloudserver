import { errors } from 'arsenal';

import services from '../services';

/**
 * objectDelete - DELETE an object from a bucket
 * (currently supports only non-versioned buckets)
 * @param {AuthInfo} authInfo - requester's infos
 * @param {object} request - request object given by router,
 *                           includes normalized headers
 * @param {Logger} log - werelogs request instance
 * @param {function} cb - final cb to call with the result and response headers
 * @return {undefined}
 */
export default function objectDelete(authInfo, request, log, cb) {
    log.debug('processing request', { method: 'objectDelete' });
    if (authInfo.isRequesterPublicUser()) {
        log.warn('operation not available for public user');
        return cb(errors.AccessDenied);
    }
    const bucketName = request.bucketName;
    const objectKey = request.objectKey;
    const valParams = {
        authInfo,
        bucketName,
        objectKey,
        requestType: 'objectDelete',
        log,
    };
    const validateHeadersParams = { headers: request.headers };
    return services.metadataValidateAuthorization(valParams,
        (err, bucket, objMD) => {
            if (err) {
                log.debug('error processing request', {
                    error: err,
                    method: 'metadataValidateAuthorization',
                });
                return cb(err);
            }
            return services.validateHeaders(objMD, validateHeadersParams,
                (err, objMD, metaHeaders) => {
                    if (err) {
                        log.debug('error from headers validation', {
                            error: err,
                            method: 'validateHeaders',
                        });
                        return cb(err);
                    }
                    if (metaHeaders['Content-Length']) {
                        log.end().addDefaultFields({
                            contentLength: metaHeaders['Content-Length'],
                        });
                    }
                    return services.deleteObject(bucketName, objMD, metaHeaders,
                        objectKey, log, cb);
                });
        });
}
