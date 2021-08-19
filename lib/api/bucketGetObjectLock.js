const { errors } = require('arsenal');
const { metadataValidateBucket } = require('../metadata/metadataUtils');
const { pushMetric } = require('../utapi/utilities');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const ObjectLockConfiguration =
  require('arsenal').models.ObjectLockConfiguration;

// Format of the xml response:
/**
 *    <ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
 *        <ObjectLockEnabled>string</ObjectLockEnabled>
 *        <Rule>
 *            <DefaultRetention>
 *                <Mode>string</Mode>
 *                <Days>integer</Days>
 *                <Years>integer</Years>
 *            </DefaultRetention>
 *        </Rule>
 *    </ObjectLockConfiguration>
 */

/**
 * bucketGetObjectLock - Return object lock configuration for the bucket
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to respond to http request
 * @return {undefined}
 */
function bucketGetObjectLock(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketGetObjectLock' });
    const { bucketName, headers, method } = request;
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: 'bucketGetObjectLock',
        request,
    };
    return metadataValidateBucket(metadataValParams, log, (err, bucket) => {
        const corsHeaders = collectCorsHeaders(headers.origin, method, bucket);
        if (err) {
            log.debug('error processing request', {
                error: err,
                method: 'bucketGetObjectLock',
            });
            return callback(err, null, corsHeaders);
        }
        const objectLockEnabled = bucket.isObjectLockEnabled();
        const bucketObjLockConfig = bucket.getObjectLockConfiguration();
        const objLockConfig = bucketObjLockConfig || {};
        if (!objectLockEnabled) {
            log.debug('object lock is not enabled', {
                error: errors.ObjectLockConfigurationNotFoundError,
                method: 'bucketGetObjectLock',
            });
            return callback(errors.ObjectLockConfigurationNotFoundError, null,
                corsHeaders);
        }
        const xml = ObjectLockConfiguration.getConfigXML(objLockConfig);
        pushMetric('getBucketObjectLock', log, {
            authInfo,
            bucket: bucketName,
        });
        return callback(null, xml, corsHeaders);
    });
}

module.exports = bucketGetObjectLock;
