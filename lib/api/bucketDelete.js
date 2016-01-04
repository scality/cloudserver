import services from '../services';
import utils from '../utils';

/**
 * bucketDelete - DELETE bucket (currently supports only non-versioned buckets)
 * @param  {string}   accessKey - user access key
 * @param  {object}   metastore - metadata storage endpoint
 * @param  {object}   request   - request object given by router
 *  including normalized headers
 * @param  {function} callback  - final callback to call
 *  with the result and response headers
 * @return {function} calls callback from router
 *  with err, result and responseMetaHeaders as args
 */
export default function bucketDelete(accessKey, metastore, request, cb) {
    if (accessKey === 'http://acs.amazonaws.com/groups/global/AllUsers') {
        return cb('AccessDenied');
    }
    const resourceRes = utils.getResourceNames(request);
    const bucketName = resourceRes.bucket;
    const metadataValParams = {
        accessKey,
        bucketName,
        metastore,
        requestType: 'bucketDelete',
    };

    services.metadataValidateAuthorization(metadataValParams, err => {
        if (err) {
            return cb(err);
        }
        services.deleteBucket(bucketName, metastore, accessKey, cb);
    });
}
