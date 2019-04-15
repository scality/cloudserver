const { zenkoIDHeader } = require('arsenal').constants;

/**
 * applyZenkoUserMD - if request is within a Zenko deployment, apply user
 * metadata called "zenko-source" to the object
 * @param {Object} metaHeaders - user metadata object
 * @return {undefined}
 */
function applyZenkoUserMD(metaHeaders) {
    if (process.env.REMOTE_MANAGEMENT_DISABLE === '0' &&
        !metaHeaders[zenkoIDHeader]) {
        // eslint-disable-next-line no-param-reassign
        metaHeaders[zenkoIDHeader] = 'zenko';
    }
}

module.exports = applyZenkoUserMD;
