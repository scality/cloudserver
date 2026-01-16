const configCache = new Map();

// Load tracking for adaptive burst capacity
// Map<bucketKey, Array<timestamp>> - rolling 1-second window
const requestTimestamps = new Map();

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
function invalidateCachedConfig(bucketName) {
    const cacheKey = `bucket:${bucketName}`;
    return configCache.delete(cacheKey);
}

module.exports = {
    setCachedConfig,
    getCachedConfig,
    expireCachedConfigs,
    invalidateCachedConfig,

    // Do not access directly
    // Used only for tests
    configCache,
    requestTimestamps,
};
