const constants = require('../conf/constants');
const ipAddressRegex = new RegExp(/^(\d+\.){3}\d+$/);
const dnsRegex = new RegExp(/^[a-z0-9]+([\.\-]{1}[a-z0-9]+)*$/);

/**
 * Validate bucket name per naming rules and restrictions
 * @param {string} bucketname - name of the bucket to be created
 * @return {boolean} - returns true/false by testing
 * bucket name against validation rules
 */
function isValidBucketName(bucketname) {
    // Must be at least 3 and no more than 63 characters long.
    if (bucketname.length < 3 || bucketname.length > 63) {
        return false;
    }
    // Must not start with the mpuBucketPrefix since this is
    // reserved for the shadow bucket used for multipart uploads
    if (bucketname.startsWith(constants.mpuBucketPrefix)) {
        return false;
    }
    // Must not contain more than one consecutive period
    if (bucketname.indexOf('..') > 1) {
        return false;
    }
    // Must not be an ip address
    if (bucketname.match(ipAddressRegex)) {
        return false;
    }
    // Must be dns compatible
    return !!bucketname.match(dnsRegex);
}

module.exports = isValidBucketName;
