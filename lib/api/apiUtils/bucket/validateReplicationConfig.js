const config = require('../../../Config').config;

/**
 * Validates that the replication configuration contains a preferred
 * read location if the bucket location is a transient source
 *
 * @param {object} repConfig - replication configuration
 * @param {object} bucket - bucket metadata
 *
 * @return {boolean} validity of replication configuration with
 * transient source
 */
function validateReplicationConfig(repConfig, bucket) {
    const bucketLocationName = bucket.getLocationConstraint();
    if (!repConfig || !repConfig.rules) {
        return false;
    }
    const bucketLocation = config.locationConstraints[bucketLocationName];
    if (!bucketLocation.isTransient) {
        return true;
    }
    return repConfig.rules.every(rule => {
        if (!rule.storageClass) {
            return true;
        }
        const storageClasses = rule.storageClass.split(',');
        return storageClasses.some(
            site => site.endsWith(':preferred_read'));
    });
}

module.exports = validateReplicationConfig;
