import services from '../services';
import utils from '../utils';

/**
 * bucketDelete - DELETE bucket (currently supports only non-versioned buckets)
 * @param  {string}   accessKey - user access key
 * @param  {object}   metastore - metadata storage endpoint
 * @param  {object}   request   - request object given by router
 *  including normalized headers
 * @param  {function} log - Werelogs log service
 * @param  {function} callback  - final callback to call
 *  with the result and response headers
 * @return {function} calls callback from router
 *  with err, result and responseMetaHeaders as args
 */
export default function bucketDelete(accessKey, metastore, request, log, cb) {
    log.debug('Processing the request in Bucket DELETE api');

    if (accessKey === 'http://acs.amazonaws.com/groups/global/AllUsers') {
        log.error('Access Denied: Operation not available for AllUsers group');
        return cb('AccessDenied');
    }

    const resourceRes = utils.getResourceNames(request);
    const bucketName = resourceRes.bucket;
    log.debug(`Bucket name: ${bucketName}`);

    const metadataValParams = {
        accessKey,
        bucketName,
        metastore,
        requestType: 'bucketDelete',
        log,
    };

    services.metadataValidateAuthorization(metadataValParams, err => {
        if (err) {
            log.error(`Error from metadata validate authorization checks: ` +
                `${err}`);
            return cb(err);
        }
        log.trace('Passed metadata validate authorization checks');
        services.deleteBucket(bucketName, metastore, accessKey, log, cb);
    });
}
