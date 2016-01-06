import services from '../services';
import utils from '../utils';

/**
 * objectDelete - DELETE an object from a bucket
 * (currently supports only non-versioned buckets)
 * @param  {string}   accessKey - user access key
 * @param  {object}   metastore - metadata storage endpoint
 * @param  {object}   request   - request object given by router,
 * includes normalized headers
 * @param  {function} cb  - final cb to call with the
 * result and response headers
 * @return {function} calls cb from router
 * with err, result and responseMetaHeaders as arguments
 */
export default
function objectDelete(accessKey, metastore, request, cb) {
    if (accessKey === 'http://acs.amazonaws.com/groups/global/AllUsers') {
        return cb('AccessDenied');
    }
    const resourceRes = utils.getResourceNames(request);
    const bucketName = resourceRes.bucket;
    const objectKey = resourceRes.object;
    const valParams = {
        accessKey,
        bucketName,
        metastore,
        objectKey,
        requestType: 'objectDelete',
    };
    const metadataCheckParams = {
        headers: request.lowerCaseHeaders
    };
    services.metadataValidateAuthorization(valParams, (err, bucket, objMD) => {
        if (err) {
            return cb(err);
        }
        services.metadataChecks(objMD, metadataCheckParams,
            (err, objMD, metaHeaders) => {
                if (err) {
                    return cb(err);
                }
                services.deleteObject(bucketName, objMD, metaHeaders,
                                      objectKey, cb);
            });
    });
}
