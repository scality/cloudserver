import async from 'async';

import services from '../services';

/**
 * HEAD Object - Same as Get Object but only respond with headers
 *(no actual body)
 * @param  {string} accessKey - user's access key
 * containing objects and their metadata
 * @param {object} request - normalized request object
 * @param  {function} log - Werelogs logger
 * @param {function} callback - callback to function in route
 * @return {function} callback with error and responseMetaHeaders as arguments
 *
 */
export default function objectHead(accessKey, request, log, callback) {
    const bucketName = request.bucketName;
    const objectKey = request.objectKey;
    const metadataValParams = {
        accessKey,
        bucketName,
        objectKey,
        requestType: 'objectHead',
        log,
    };
    const validateHeadersParams = {headers: request.lowerCaseHeaders};

    async.waterfall([
        function waterfall1(next) {
            services.metadataValidateAuthorization(metadataValParams, next);
        },
        function waterfall2(bucket, objectMetadata, next) {
            services.validateHeaders(objectMetadata, validateHeadersParams,
                next);
        }
    ], function finalfunc(err, objectMetadata, responseMetaHeaders) {
        return callback(err, responseMetaHeaders);
    });
}
