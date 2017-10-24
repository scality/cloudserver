const s3config = require('../../../Config').config;

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
            const { storageClass } = rule;
            const location = s3config.locationConstraints[storageClass];
            if (location && location.type === 'aws_s3' && operationType) {
                content.push(operationType);
            }
            return {
                status: 'PENDING',
                content,
                destination: config.destination,
                storageClass: storageClass || '',
                role: config.role,
                storageType: location && location.type || '',
            };
        }
    }
    return undefined;
}

module.exports = getReplicationInfo;
