const s3config = require('../../../Config').config;
const { isServiceAccount, getServiceAccountProperties } =
      require('../authorization/aclChecks');
const { replicationBackends } = require('arsenal').constants;

function _getBackend(objectMD, site) {
    const backends = objectMD ? objectMD.replicationInfo.backends : [];
    const backend = backends.find(o => o.site === site);
    // If the backend already exists, just update the status.
    if (backend) {
        return Object.assign({}, backend, { status: 'PENDING' });
    }
    return {
        site,
        status: 'PENDING',
        dataStoreVersionId: '',
    };
}

function _getStorageClasses(rule) {
    if (rule.storageClass) {
        return rule.storageClass.split(',');
    }
    const { replicationEndpoints } = s3config;
    // If no storage class, use the given default endpoint or the sole endpoint
    if (replicationEndpoints.length > 1) {
        const endPoint =
            replicationEndpoints.find(endpoint => endpoint.default);
        return [endPoint.site];
    }
    return [replicationEndpoints[0].site];
}

function _getReplicationInfo(rule, replicationConfig, content, operationType,
    objectMD, bucketMD) {
    const storageTypes = [];
    const backends = [];
    const storageClasses = _getStorageClasses(rule);
    storageClasses.forEach(storageClass => {
        const storageClassName =
              storageClass.endsWith(':preferred_read') ?
              storageClass.split(':')[0] : storageClass;
        const location = s3config.locationConstraints[storageClassName];
        if (location && replicationBackends[location.type]) {
            storageTypes.push(location.type);
        }
        backends.push(_getBackend(objectMD, storageClassName));
    });
    if (storageTypes.length > 0 && operationType) {
        content.push(operationType);
    }
    return {
        status: 'PENDING',
        backends,
        content,
        destination: replicationConfig.destination,
        storageClass: storageClasses.join(','),
        role: replicationConfig.role,
        storageType: storageTypes.join(','),
        isNFS: bucketMD.isNFS(),
    };
}

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
    // - object owner is an internal service account like Lifecycle,
    //   unless the account properties explicitly allow it to
    //   replicate like MD ingestion (because we do not want to
    //   replicate objects created from actions triggered by internal
    //   services, by design)

    if (config) {
        let doReplicate = false;
        if (!authInfo || !isServiceAccount(authInfo.getCanonicalID())) {
            doReplicate = true;
        } else {
            const serviceAccountProps = getServiceAccountProperties(
                authInfo.getCanonicalID());
            doReplicate = serviceAccountProps.canReplicate;
        }
        if (doReplicate) {
            const rule = config.rules.find(
                rule => (objKey.startsWith(rule.prefix) && rule.enabled));
            if (rule) {
                return _getReplicationInfo(
                    rule, config, content, operationType, objectMD, bucketMD);
            }
        }
    }
    return undefined;
}

module.exports = getReplicationInfo;
