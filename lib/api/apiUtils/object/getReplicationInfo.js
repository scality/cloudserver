const s3config = require('../../../Config').config;

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
    objectMD) {
    const storageTypes = [];
    const backends = [];
    const storageClasses = _getStorageClasses(rule);
    storageClasses.forEach(storageClass => {
        const location = s3config.locationConstraints[storageClass];
        if (location && ['aws_s3', 'azure'].includes(location.type)) {
            storageTypes.push(location.type);
        }
        backends.push(_getBackend(objectMD, storageClass));
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
 * @return {undefined}
 */
function getReplicationInfo(objKey, bucketMD, isMD, objSize, operationType,
    objectMD) {
    const content = isMD || objSize === 0 ? ['METADATA'] : ['DATA', 'METADATA'];
    const config = bucketMD.getReplicationConfiguration();
    // If bucket does not have a replication configuration, do not replicate.
    if (config) {
        const rule = config.rules.find(rule => objKey.startsWith(rule.prefix));
        if (rule) {
            return _getReplicationInfo(rule, config, content, operationType,
                objectMD);
        }
    }
    return undefined;
}

module.exports = getReplicationInfo;
