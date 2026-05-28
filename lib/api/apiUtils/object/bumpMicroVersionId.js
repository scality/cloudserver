const { versioning } = require('arsenal');
const { config } = require('../../../Config');

/**
 * Bump objectMD.microVersionId. microVersionId is a generic
 * metadata-revision marker, not a CRR-specific field, but cascaded CRR
 * is its only consumer today - so we gate on replicationInfo to avoid
 * inflating storage on objects that wouldn't use it. The gate can be
 * widened later if another consumer needs it on every object.
 * Pass `force = true` to bump unconditionally.
 *
 * @param {object} objectMD - object MD POJO or `md.getValue()`
 * @param {boolean} [force] - bump even without replicationInfo
 * @return {undefined}
 */
function bumpMicroVersionId(objectMD, force) {
    if (!force && !objectMD?.replicationInfo) {
        return;
    }

    const { instanceId, replicationGroupId } = config;

    // eslint-disable-next-line no-param-reassign
    objectMD.microVersionId = versioning.VersionID.generateVersionId(instanceId, replicationGroupId);
}

module.exports = bumpMicroVersionId;
