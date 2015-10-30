import utils from '../utils';
import services from '../services';

/**
 * Determine if bucket exists and if user has permission to access it
 * @param  {string} accessKey - user's accessKey
 * @param {object} metastore - metadata store
 * @param  {object} request - http request object
 * @param  {function} callback - callback to respond to http request
 *  with either error code or success
 * @return {callbackReturn} - return of the callback
 */
export default function bucketHead(accessKey, metastore, request, callback) {
    const bucketname = utils.getResourceNames(request).bucket;
    const metadataValParams = {
        accessKey: accessKey,
        bucketUID: utils.getResourceUID(request.namespace, bucketname),
        metastore: metastore,
    };
    services.metadataValidateAuthorization(metadataValParams, (err) => {
        return callback(err, "Bucket exists and user authorized -- 200");
    });
}
