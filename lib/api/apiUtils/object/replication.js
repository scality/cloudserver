/**
 * Check that a bucket replication rule applies for the given object key. If it
 * does, assign replication information to the given object.
 * @param {object} obj - The object to set replicationInfo for
 * @param {string} objKey - The key of the object
 * @param {object} bucketMD - The bucket metadata
 * @param {array} content - The content type that should be replicated
 * @return {undefined}
 */
function buildReplicationInfo(obj, objKey, bucketMD, content) {
    const config = bucketMD.getReplicationConfiguration();
    // If bucket does not have a replication configuration, do not replicate.
    if (config) {
        const rule = config.rules.find(rule => objKey.startsWith(rule.prefix));
        if (rule) {
            // eslint-disable-next-line no-param-reassign
            obj.replicationInfo = {
                status: 'PENDING',
                content,
                destination: config.destination,
                storageClass: rule.storageClass || '',
            };
        }
    }
    return undefined;
}

/**
 * Set the object replicationInfo to replicate data and metadata, or only
 * metadata if the object is a delete marker
 * @param {object} obj - The object to set replicationInfo for
 * @param {string} objKey - The key of the object
 * @param {object} bucketMD - The bucket metadata
 * @param {boolean} isDeleteMarker - Whether the object is a deleteMarker
 * @return {undefined}
 */
function buildReplicationInfoForObject(obj, objKey, bucketMD, isDeleteMarker) {
    // Delete markers have no data, so we only replicate metadata.
    const content = isDeleteMarker ? ['METADATA'] : ['DATA', 'METADATA'];
    return buildReplicationInfo(obj, objKey, bucketMD, content);
}

/**
 * Set the object replicationInfo to replicate only metadata
 * @param {object} obj - The object to set replicationInfo for
 * @param {string} objKey - The key of the object
 * @param {object} bucketMD - The bucket metadata
 * @return {undefined}
 */
function buildReplicationInfoForObjectMD(obj, objKey, bucketMD) {
    return buildReplicationInfo(obj, objKey, bucketMD, ['METADATA']);
}

module.exports = {
    buildReplicationInfoForObject,
    buildReplicationInfoForObjectMD,
};
