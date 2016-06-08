import async from 'async';

import services from '../services';

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
    const validateHeadersParams = { headers: request.headers };

    async.waterfall([
        next => services.metadataValidateAuthorization(metadataValParams, next),
        (bucket, objectMetadata, next) => services.validateHeaders(
                                objectMetadata, validateHeadersParams, next),
    ], (err, objectMetadata, responseMetaHeaders) =>
        callback(err, responseMetaHeaders));
}
