import { errors } from 'arsenal';

import services from '../services';
import validateHeaders from '../utilities/validateHeaders';


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
        query: request.query,
    };
    return services.metadataValidateAuthorization(valParams,
        (err, bucket, objMD) => {
            if (err) {
                log.debug('error processing request', {
                    error: err,
                    method: 'metadataValidateAuthorization',
                });
                return cb(err);
            }
            if (!objMD) {
                return cb(errors.NoSuchKey);
            }
            const headerValResult = validateHeaders(objMD, request.headers);
            if (headerValResult.error) {
                return cb(headerValResult.error);
            }
            if (objMD['content-length']) {
                log.end().addDefaultFields({
                    contentLength: objMD['content-length'],
                });
            }
            const params = {
                versioning: bucket.isVersioningOn(),
                versionId: request.query ? request.query.versionId : undefined,
            };
            return services.deleteObject(bucketName, objMD, objectKey, log,
                err => {
                    if (err) {
                        return cb(err);
                    }
                    return cb(err, objMD['content-length']);
                }, params);
        });
}
