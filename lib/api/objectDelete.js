import services from '../services';

/**
 * objectDelete - DELETE an object from a bucket
 * (currently supports only non-versioned buckets)
 * @param  {string}   accessKey - user access key
 * @param  {object}   request   - request object given by router,
 * includes normalized headers
 * @param  {function} cb  - final cb to call with the
 * result and response headers
 * @return {function} calls cb from router
 * with err, result and responseMetaHeaders as arguments
 */
export default function objectDelete(accessKey, request, log, cb) {
    log.debug('Processing the request in Object DELETE api');
    if (accessKey === 'http://acs.amazonaws.com/groups/global/AllUsers') {
        log.error('Access Denied: Operation not available for AllUsers group');
        return cb('AccessDenied');
    }
    const bucketName = request.bucketName;
    log.debug(`Bucket Name: ${bucketName}`);
    const objectKey = request.objectKey;
    log.debug(`Object Key: ${objectKey}`);
    const valParams = {
        accessKey,
        bucketName,
        objectKey,
        requestType: 'objectDelete',
        log,
    };
    const validateHeadersParams = {
        headers: request.lowerCaseHeaders
    };
    services.metadataValidateAuthorization(valParams, (err, bucket, objMD) => {
        if (err) {
            log.error(`Error from metadata validate authorization checks: ` +
                `${err}`);
            return cb(err);
        }
        services.validateHeaders(objMD, validateHeadersParams,
            (err, objMD, metaHeaders) => {
                if (err) {
                    log.error(`Error from headers validation: ${err}`);
                    return cb(err);
                }
                services.deleteObject(bucketName, objMD, metaHeaders, objectKey,
                    log, cb);
            });
    });
}
