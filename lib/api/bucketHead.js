import utils from '../utils.js';
import services from '../services.js';

/**
 * bucketHead - Determine if bucket exists and
 * if user has permission to access it
 * @param  {string} accessKey - user's accessKey
 * @param {object} metastore - metadata store
 * @param  {object} request - http request object
 * @param  {function} callback - callback to respond to http request
 *  with either error code or success
 */

export default function bucketHead(accessKey, metastore, request, callback) {
    const bucketname = utils.getResourceNames(request).bucket;
    const bucketUID = utils.getResourceUID(request.namespace, bucketname);
    const metadataValParams = {accessKey: accessKey,
        bucketUID: bucketUID, metastore: metastore};
    services.metadataValidateAuthorization(
        metadataValParams, function (err, bucket) {
            return callback(err, "Bucket exists and user authorized -- 200");
        });
}
