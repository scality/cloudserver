const s3config = require('../../../Config').config;

function _getReplicationInfo(rule, replicationConfig, content, operationType) {
    const storageClasses = rule.storageClass ?
        rule.storageClass.split(',') : [];
    const storageTypes = [];
    const backends = [];
    storageClasses.forEach(storageClass => {
        const location = s3config.locationConstraints[storageClass];
        if (location && ['aws_s3', 'azure'].includes(location.type)) {
            storageTypes.push(location.type);
        }
        backends.push({
            site: storageClass,
            status: 'PENDING',
        });
    });
    if (storageTypes.length > 0 && operationType) {
        content.push(operationType);
    }
    // If no StorageClass, we replicate to the sole Scality S3 destination.
    if (backends.length === 0) {
        const dest = s3config.replicationEndpoints &&
            s3config.replicationEndpoints.find(site =>
            site.servers !== undefined);
        backends.push({
            site: dest && dest.site,
            status: 'PENDING',
        });
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
 * @return {undefined}
 */
function getReplicationInfo(objKey, bucketMD, isMD, objSize, operationType) {
    const content = isMD || objSize === 0 ? ['METADATA'] : ['DATA', 'METADATA'];
    const config = bucketMD.getReplicationConfiguration();
    // If bucket does not have a replication configuration, do not replicate.
    if (config) {
        const rule = config.rules.find(rule => objKey.startsWith(rule.prefix));
        if (rule) {
            return _getReplicationInfo(rule, config, content, operationType);
        }
    }
    return undefined;
}

module.exports = getReplicationInfo;
