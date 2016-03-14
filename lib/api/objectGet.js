import { errors } from 'arsenal';
import async from 'async';

import services from '../services';

/**
 * GET Object - Get an object
 * @param  {AuthInfo} Instance of AuthInfo class with requester's info
 * @param {object} request - normalized request object
 * @param {function} callback - callback to function in route
 * @return {function} callback with error, object data result
 * and responseMetaHeaders as arguments
 */
export default
function objectGet(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'objectGet' });
    const bucketName = request.bucketName;
    const objectKey = request.objectKey;
    const metadataValParams = {
        authInfo,
        bucketName,
        objectKey,
        requestType: 'objectGet',
        log,
    };
    const validateHeadersParams = { headers: request.headers };

    async.waterfall([
        function waterfall1(next) {
            services.metadataValidateAuthorization(metadataValParams,
                (err, bucket, objectMetadata) => {
                    if (!objectMetadata) {
                        log.warn('error processing request', {
                            error: err,
                            method: 'metadataValidateAuthorization',
                        });
                        return next(errors.NoSuchKey);
                    }
                    return next(null, bucket, objectMetadata);
                });
        },
        function waterfall2(bucket, objectMetadata, next) {
            services.validateHeaders(objectMetadata, validateHeadersParams,
                next);
        },
        function waterfall3(objectMetadata, metaHeaders, next) {
            services.getFromDatastore(objectMetadata, metaHeaders, log, next);
        },
    ], function waterfallFinal(err, result, responseMetaHeaders) {
        return callback(err, result, responseMetaHeaders);
    });
}
