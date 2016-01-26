import services from '../services';

/**
 * Determine if bucket exists and if user has permission to access it
 * @param  {string} accessKey - user's accessKey
 * @param {object} metastore - metadata store
 * @param  {object} request - http request object
 * @param  {function} log - Werelogs logger
 * @param  {function} callback - callback to respond to http request
 *  with either error code or success
 * @return {callbackReturn} - return of the callback
 */
export default function bucketHead(accessKey, metastore, request, log,
    callback) {
    log.debug('Processing the request in Bucket HEAD api');
    const bucketName = request.bucketName;
    log.debug(`Bucket Name: ${bucketName}`);
    const metadataValParams = {
        accessKey,
        metastore,
        bucketName,
        requestType: 'bucketHead',
        log,
    };
    services.metadataValidateAuthorization(metadataValParams, (err) => {
        return callback(err, "Bucket exists and user authorized -- 200");
    });
}
