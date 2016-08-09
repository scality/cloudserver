import errors from 'arsenal';

import collectResponseHeaders from '../utilities/collectResponseHeaders';
import services from '../services';
import validateHeaders from '../utilities/validateHeaders';

/**
 * HEAD Object - Same as Get Object but only respond with headers
 *(no actual body)
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - normalized request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to function in route
 * @return {undefined}
 *
 */
export default function objectHead(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'objectHead' });
    const bucketName = request.bucketName;
    const objectKey = request.objectKey;
    const metadataValParams = {
        authInfo,
        bucketName,
        objectKey,
        requestType: 'objectHead',
        log,
    };

    return services.metadataValidateAuthorization(metadataValParams,
        (err, bucket, objMD) => {
            if (err) {
                log.debug('error processing request', {
                    error: err,
                    method: 'metadataValidateAuthorization',
                });
                return callback(err);
            }
            if (!objMD) {
                return callback(errors.NoSuchKey);
            }
            const headerValResult = validateHeaders(objMD, request.headers);
            if (headerValResult.error) {
                return callback(headerValResult.error);
            }
            const responseMetaHeaders = collectResponseHeaders(objMD);
            return callback(err, responseMetaHeaders);
        });
}
