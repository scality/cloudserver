const counters = new Map();

const configCache = new Map();

// Dirty tracking for Redis synchronization
const dirtyCounters = new Set();
const lastSyncedValues = new Map();

// Load tracking for adaptive burst capacity
// Map<bucketKey, Array<timestamp>> - rolling 1-second window
const requestTimestamps = new Map();

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

    return toRemove.length;
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

    return toRemove.length;
}

/**
 * Mark a counter as dirty (needs Redis sync)
 * @param {string} key - Counter key
 */
function markDirty(key) {
    dirtyCounters.add(key);
}

/**
 * Get all dirty counter keys
 * @returns {string[]} Array of dirty keys
 */
function getDirtyKeys() {
    return Array.from(dirtyCounters);
}

/**
 * Clear all dirty counter tracking
 */
function clearDirtyKeys() {
    dirtyCounters.clear();
}

/**
 * Get last synced value for a counter
 * @param {string} key - Counter key
 * @returns {number|undefined} Last synced emptyAt timestamp
 */
function getLastSyncedValue(key) {
    return lastSyncedValues.get(key);
}

/**
 * Set last synced value for a counter
 * @param {string} key - Counter key
 * @param {number} value - emptyAt timestamp
 */
function setLastSyncedValue(key, value) {
    lastSyncedValues.set(key, value);
}

/**
 * Remove last synced value tracking for a key
 * @param {string} key - Counter key
 */
function deleteLastSyncedValue(key) {
    lastSyncedValues.delete(key);
}

/**
 * Record a request for load tracking
 * @param {string} key - Bucket key (e.g., "bucket:mybucket:rps")
 */
function recordRequest(key) {
    const now = Date.now();
    let timestamps = requestTimestamps.get(key);

    if (!timestamps) {
        timestamps = [];
        requestTimestamps.set(key, timestamps);
    }

    timestamps.push(now);

    // Remove timestamps older than 1 second
    const cutoff = now - 1000;
    while (timestamps.length > 0 && timestamps[0] < cutoff) {
        timestamps.shift();
    }
}

/**
 * Calculate current request rate for a bucket
 * @param {string} key - Bucket key
 * @returns {number} Requests per second in the last 1 second window
 */
function getCurrentRate(key) {
    const timestamps = requestTimestamps.get(key);
    if (!timestamps || timestamps.length === 0) {
        return 0;
    }

    const now = Date.now();
    const cutoff = now - 1000;

    // Count requests in last second
    let count = 0;
    for (let i = timestamps.length - 1; i >= 0; i--) {
        if (timestamps[i] >= cutoff) {
            count++;
        } else {
            break;
        }
    }

    return count;
}

/**
 * Calculate load factor (current rate / target rate)
 * @param {string} key - Bucket key
 * @param {number} targetRate - Target requests per second
 * @returns {number} Load factor (1.0 = normal, >1.0 = overload)
 */
function getLoadFactor(key, targetRate) {
    const currentRate = getCurrentRate(key);
    if (targetRate === 0) {
        return 0;
    }
    return currentRate / targetRate;
}

/**
 * Expire request timestamps for cleanup
 * @param {number} now - Current timestamp
 */
function expireRequestTimestamps(now) {
    const cutoff = now - 2000; // Keep 2 seconds of history

    for (const [key, timestamps] of requestTimestamps.entries()) {
        while (timestamps.length > 0 && timestamps[0] < cutoff) {
            timestamps.shift();
        }

        if (timestamps.length === 0) {
            requestTimestamps.delete(key);
        }
    }
}

module.exports = {
    setCounter,
    getCounter,
    expireCounters,
    setCachedConfig,
    getCachedConfig,
    expireCachedConfigs,
    markDirty,
    getDirtyKeys,
    clearDirtyKeys,
    getLastSyncedValue,
    setLastSyncedValue,
    deleteLastSyncedValue,
    recordRequest,
    getCurrentRate,
    getLoadFactor,
    expireRequestTimestamps,

    // Do not access directly
    // Used only for tests
    counters,
    configCache,
    dirtyCounters,
    lastSyncedValues,
    requestTimestamps,
};
