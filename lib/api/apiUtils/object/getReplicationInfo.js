const s3config = require('../../../Config').config;
const { isBackbeatUser } = require('../authorization/aclChecks');
// const { replicationBackends } = require('arsenal').constants;
const { getReplicationInfoObject } =
    require('arsenal').s3middleware.replicationInfo;

/**
 * Get the object replicationInfo to replicate data and metadata, or only
 * metadata if the operation only changes metadata or the object is 0 bytes
 * @param {string} objKey - The key of the object
 * @param {object} bucketMD - The bucket metadata
 * @param {boolean} isMD - Whether the operation is only updating metadata
 * @param {boolean} objSize - The size, in bytes, of the object being PUT
 * @param {string} operationType - The type of operation to replicate
 * @param {object} objectMD - The object metadata
 * @param {AuthInfo} [authInfo] - authentication info of object owner
 * @return {undefined}
 */
function getReplicationInfo(objKey, bucketMD, isMD, objSize, operationType,
    objectMD, authInfo) {
    const content = isMD || objSize === 0 ? ['METADATA'] : ['DATA', 'METADATA'];
    const config = bucketMD.getReplicationConfiguration();

    // Do not replicate object in the following cases:
    //
    // - bucket does not have a replication configuration
    //
    // - replication configuration does not apply to the object
    //   (i.e. no rule matches object prefix)
    //
    // - replication configuration applies to the object (i.e. a rule matches
    //   object prefix) but the status is disabled
    //
    // - object owner is an internal service account like Lifecycle
    //   (because we do not want to replicate objects created from
    //   actions triggered by internal services, by design)

    if (config && (!authInfo || !isBackbeatUser(authInfo.getCanonicalID()))) {
        const rule = config.rules.find(rule =>
            (objKey.startsWith(rule.prefix) && rule.enabled));
        if (rule) {
            return getReplicationInfoObject(rule, config, content,
                operationType, objectMD, bucketMD, s3config);
        }
    }
    return undefined;
}

module.exports = getReplicationInfo;
