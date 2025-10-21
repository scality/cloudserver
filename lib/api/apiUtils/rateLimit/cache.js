const counters = new Map();

const configCache = new Map();

function setCounter(key, value) {
    // Make sure that the Map remains in order
    // Counters expiring soonest will be first during iteration.
    counters.delete(key);
    counters.set(key, value);
}

function getCounter(key) {
    return counters.get(key);
}

function expireCounters(now) {
    const toRemove = [];
    for (const [key, value] of counters.entries()) {
        if (value <= now) {
            toRemove.push(key);
        }
    }

    for (const key of toRemove) {
        counters.delete(key);
    }
}

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
}

module.exports = {
    setCounter,
    getCounter,
    expireCounters,
    setCachedConfig,
    getCachedConfig,
    expireCachedConfigs,

    // Do not access directly
    // Used only for tests
    counters,
    configCache,
};
