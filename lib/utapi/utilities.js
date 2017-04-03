import { UtapiClient } from 'utapi';
import _config from '../Config';
// setup utapi client
const utapi = new UtapiClient({
    utapiEnabled: _config.utapiEnabled,
    component: 's3',
});

/**
 * Call the Utapi Client `pushMetric` method with the associated parameters
 * @param {string} action - the metric action to push a metric for
 * @param {object} log - werelogs logger
 * @param {object} metricObj - the object containing the relevant data for
 * pushing metrics in Utapi
 * @param {string} [metricObj.bucket] - (optional) bucket name
 * @param {AuthInfo} [metricObj.authInfo] - (optional) Instance of AuthInfo
 * class with requester's info
 * @param {number} [metricObj.byteLength] - (optional) current object size
 * (used, for example, for pushing 'deleteObject' metrics)
 * @param {number} [metricObj.newByteLength] - (optional) new object size
 * @param {number|null} [metricObj.oldByteLength] - (optional) old object size
 * (obj. overwrites)
 * @param {number} [metricObj.numberOfObjects] - (optional) number of obects
 * added/deleted
 * @return {function} - `utapi.pushMetric`
 */
export function pushMetric(action, log, metricObj) {
    const { bucket, byteLength, newByteLength, oldByteLength, numberOfObjects,
        authInfo } = metricObj;
    const utapiObj = {
        bucket,
        byteLength,
        newByteLength,
        oldByteLength,
        numberOfObjects,
    };
    // If `authInfo` is included by the API, get the account's canonical ID for
    // account-level metrics and the shortId for user-level metrics.
    if (authInfo) {
        utapiObj.accountId = authInfo.getCanonicalID();
        utapiObj.userId = authInfo.isRequesterAnIAMUser() ?
            authInfo.getShortid() : undefined;
    }
    return utapi.pushMetric(action, log.getSerializedUids(), utapiObj);
}
