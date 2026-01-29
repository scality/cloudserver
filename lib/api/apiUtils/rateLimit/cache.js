const configCache = new Map();

function setCachedConfig(key, limitConfig, ttl) {
    const expiry = Date.now() + ttl;
    configCache.set(key, { expiry, config: limitConfig });
}

function getCachedConfig(key) {
    const value = configCache.get(key);
    if (value === undefined) {
        return undefined;
    }

    const { expiry, config } = value;
    if (expiry <= Date.now()) {
        configCache.delete(key);
        return undefined;
    }

    return config;
}

function expireCachedConfigs(now) {
    const toRemove = [];
    for (const [key, { expiry }] of configCache.entries()) {
        if (expiry <= now) {
            toRemove.push(key);
        }
    }

    for (const key of toRemove) {
        configCache.delete(key);
    }

    return toRemove.length;
}

/**
 * Invalidate cached config for a specific bucket
 *
 * @param {string} bucketName - Bucket name
 * @returns {boolean} True if entry was found and removed
 */
function deleteCachedConfig(key) {
    return configCache.delete(key);
}

module.exports = {
    setCachedConfig,
    getCachedConfig,
    expireCachedConfigs,
    // TODO change this wherever
    invalidateCachedConfig: deleteCachedConfig,
    deleteCachedConfig,

    // Do not access directly
    // Used only for tests
    configCache,
    requestTimestamps,
};
