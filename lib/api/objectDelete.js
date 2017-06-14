import { errors } from 'arsenal';

import collectCorsHeaders from '../utilities/collectCorsHeaders';
import services from '../services';
import { pushMetric } from '../utapi/utilities';


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
    return services.metadataValidateAuthorization(valParams,
        (err, bucket, objMD) => {
            const corsHeaders = collectCorsHeaders(request.headers.origin,
                request.method, bucket);
            if (err) {
                log.debug('error processing request', {
                    error: err,
                    method: 'metadataValidateAuthorization',
                });
                return cb(err, corsHeaders);
            }
            if (!objMD) {
                return cb(errors.NoSuchKey, corsHeaders);
            }
            if (objMD['content-length']) {
                log.end().addDefaultFields({
                    contentLength: objMD['content-length'],
                });
            }
            return services.deleteObject(bucketName, objMD, objectKey, log,
                err => {
                    if (err) {
                        return cb(err, corsHeaders);
                    }
                    pushMetric('deleteObject', log, {
                        authInfo,
                        bucket: bucketName,
                        byteLength: objMD['content-length'],
                        numberOfObjects: 1,
                    });
                    return cb(null, corsHeaders);
                });
        });
}
