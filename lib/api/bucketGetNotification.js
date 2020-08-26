const { metadataValidateBucket } = require('../metadata/metadataUtils');
const { pushMetric } = require('../utapi/utilities');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const { NotificationConfiguration } = require('arsenal').models;

/**
 * Format of xml response:
 *
 * <ONotificationConfiguration>
 *      <QueueConfiguration>
 *          <Event>array</Event>
 *          <Filter>
 *              <S3Key>
 *                  <FilterRule>
 *                      <Name>string</Name>
 *                      <Value>string</Value>
 *                  </FilterRule>
 *              </S3Key>
 *          </Filter>
 *          <Id>string</Id>
 *          <Queue>string</Queue>
 *      </QueueConfiguration>
 * </NotificationConfiguration>
 */

/**
 * bucketGetNotification - Return notification configuration for the bucket
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to respond to http request
 * @return {undefined}
 */
function bucketGetNotification(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketGetNotification' });
    const { bucketName, headers, method } = request;
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: 'bucketGetNotification',
    };

    return metadataValidateBucket(metadataValParams, log, (err, bucket) => {
        const corsHeaders = collectCorsHeaders(headers.origin, method, bucket);
        if (err) {
            log.debug('error processing request', {
                error: err,
                method: 'bucketGetNotification',
            });
            return callback(err, null, corsHeaders);
        }
        const bucketNotifConfig = bucket.getNotificationConfiguration();
        const notifXml = NotificationConfiguration.getConfigXML(bucketNotifConfig);
        // TODO: implement Utapi metric support
        // bucketPolicy needs to be JSON stringified on return for proper
        // parsing on return to caller function
        pushMetric('getBucketNotification', log, {
            authInfo,
            bucket: bucketName,
        });    
        return callback(null, notifXml, corsHeaders);
    });
}

module.exports = bucketGetNotification;
