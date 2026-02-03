const configCache = new Map();
const bucketOwnerCache = new Map();

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

const getCachedConfig = cacheGet.bind(null, configCache);
const setCachedConfig = cacheSet.bind(null, configCache);
const deleteCachedConfig = cacheDelete.bind(null, configCache);
const expireCachedConfigs = cacheExpire.bind(null, configCache);

const getCachedBucketOwner = cacheGet.bind(null, bucketOwnerCache);
const setCachedBucketOwner = cacheSet.bind(null, bucketOwnerCache);
const deleteCachedBucketOwner = cacheDelete.bind(null, bucketOwnerCache);
const expireCachedBucketOwners = cacheExpire.bind(null, bucketOwnerCache);

module.exports = {
    setCachedConfig,
    getCachedConfig,
    expireCachedConfigs,
    deleteCachedConfig,
    setCachedBucketOwner,
    getCachedBucketOwner,
    deleteCachedBucketOwner,

    expireCachedBucketOwners,
    // Do not access directly
    // Used only for tests
    configCache,
    bucketOwnerCache,
};
