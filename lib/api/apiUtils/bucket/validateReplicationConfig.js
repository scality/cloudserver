/**
 * Validates that the replication configuration will contain the default
 * read location as a site if the read and write locations are different
 * @param {object} config - replication configuration
 * @param {object} bucket - bucket metadata
 * @return {boolean} validity of replication configuration with rest endpoint
 * configuration
 */
function validateConfiguration(config, bucket) {
    const writeLocation = bucket.getLocationConstraint();
    const readLocation = bucket.getReadLocationConstraint();
    if (!config || !config.rules) {
        return false;
    }
    const isValid = config.rules.some(rule => {
        if (!rule.storageClass) {
            return false;
        }
        const storageClasses = rule.storageClass.split(',');
        return storageClasses.some(site => site === readLocation);
    });
    return (writeLocation === readLocation) || isValid;
}

module.exports = validateConfiguration;
