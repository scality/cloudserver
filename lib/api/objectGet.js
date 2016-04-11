import services from '../services';

/**
 * GET Object - Get an object
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - normalized request object
 * @param {object} log - Werelogs instance
 * @param {function} callback - callback to function in route
 * @return {undefined}
 */
export default
function objectGet(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'objectGet' });
    const bucketName = request.bucketName;
    const objectKey = request.objectKey;
    const mdValParams = {
        authInfo,
        bucketName,
        objectKey,
        requestType: 'objectGet',
        log,
    };
    services.metadataValidateAuthorization(mdValParams, (err, bucket,
        objMD) => {
        if (err) {
            log.debug('error processing request', { error: err });
            return callback(err);
        }
        services.validateHeaders(objMD, request, (error, objMD,
            responseMetaHeaders) => {
            if (error) {
                log.debug('error processing request', { error });
                return callback(error);
            }
            // 0 bytes file
            if (objMD.location === null) {
                return callback(null, null, responseMetaHeaders);
            }
            return services.getFromDatastore(objMD, responseMetaHeaders,
                log, callback);
        });
        return undefined;
    });
}
