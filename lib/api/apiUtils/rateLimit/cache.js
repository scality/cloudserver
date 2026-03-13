const configCache = new Map();

const namespace = {
    bucket: 'bkt',
};

function cacheSet(cache, key, value, ttl) {
    const expiry = Date.now() + ttl;
    cache.set(key, { expiry, value });
}

function cacheGet(cache, key) {
    const cachedValue = cache.get(key);
    if (cachedValue === undefined) {
        return undefined;
    }

    const { expiry, value } = cachedValue;
    if (expiry <= Date.now()) {
        cache.delete(key);
        return undefined;
    }

    return value;
}

function cacheDelete(cache, key) {
    cache.delete(key);
}

function cacheExpire(cache) {
    const now = Date.now();

    const toRemove = [];
    for (const [key, { expiry }] of cache.entries()) {
        if (expiry <= now) {
            toRemove.push(key);
        }
    }

    for (const key of toRemove) {
        cache.delete(key);
    }

    return toRemove.length;
}

function formatKeyDecorator(fn) {
    return (resourceClass, resourceId, ...args) => fn(`${resourceClass}:${resourceId}`, ...args);
}

const getCachedConfig = formatKeyDecorator(cacheGet.bind(null, configCache));
const setCachedConfig = formatKeyDecorator(cacheSet.bind(null, configCache));
const deleteCachedConfig = formatKeyDecorator(cacheDelete.bind(null, configCache));
const expireCachedConfigs = cacheExpire.bind(null, configCache);

module.exports = {
    namespace,
    setCachedConfig,
    getCachedConfig,
    expireCachedConfigs,
    deleteCachedConfig,
    // Do not access directly
    // Used only for tests
    configCache,
};
