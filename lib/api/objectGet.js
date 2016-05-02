import services from '../services';
import { errors } from 'arsenal';
import { validateRange } from './apiUtils/object/validateRange';


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
    let range;
    if (request.headers.range) {
        range = validateRange(request.headers.range);
        if (range.length === 0) {
            return callback(errors.InvalidRange);
        }
    }
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
            // To provide for backwards compatibility before md-model-version 2,
            // need to handle cases where objMD.location is just a string
            const dataLocator = Array.isArray(objMD.location) ?
                objMD.location : [objMD.location];
            return callback(null, dataLocator, responseMetaHeaders);
        });
        return undefined;
    });
}
