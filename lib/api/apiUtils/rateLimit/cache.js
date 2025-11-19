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

module.exports = {
    setCachedConfig,
    getCachedConfig,
    expireCachedConfigs,

    // Do not access directly
    // Used only for tests
    configCache,
};
