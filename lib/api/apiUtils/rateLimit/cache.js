/**
 * Rate Limit Counter Cache (Simplified from POC)
 *
 * Stores GCRA counters in memory for zero-latency rate limiting decisions.
 * Counters are simple numbers (TAT timestamps), not complex objects.
 *
 * COUNTER STORAGE:
 *   - Each counter stores a single number: TAT (Theoretical Arrival Time)
 *   - TAT is a timestamp in ms representing when the rate limit bucket empties
 *   - No additional metadata needed (interval calculated on-the-fly)
 *
 * ORDERED MAP:
 *   - Map maintains insertion order
 *   - Counters naturally ordered by when they were last updated
 *   - Efficient expiry: scan from oldest to newest, stop at first future TAT
 *
 * WHY SIMPLIFIED?
 *   - POC uses simple numbers: proven to work correctly
 *   - Less memory overhead (number vs object)
 *   - Simpler code, fewer bugs
 *   - interval and lastSync not needed for algorithm correctness
 */

const counters = new Map();
const configCache = new Map();

/**
 * Set counter value (simple number, not object)
 *
 * @param {string} key - Counter key (e.g., "throttling:bucket:my-bucket:rps")
 * @param {number} value - TAT timestamp in milliseconds
 */
function setCounter(key, value) {
    // Delete first to maintain insertion order
    // Counters updated most recently will be last
    counters.delete(key);
    counters.set(key, value);
}

/**
 * Get counter value
 *
 * Returns Date.now() if counter doesn't exist (POC behavior).
 * This naturally handles cold start: first request gets current time as TAT.
 *
 * @param {string} key - Counter key
 * @returns {number} TAT timestamp in milliseconds
 */
function getCounter(key) {
    const value = counters.get(key);
    if (value === undefined) {
        return Date.now();  // POC behavior: default to now
    }
    return value;
}

/**
 * Expire old counters (simplified for numbers)
 *
 * Removes counters where TAT is in the past (plus 10s buffer).
 * Map is ordered by insertion, so we scan oldest first and stop when
 * we hit a future TAT (all subsequent counters are also future).
 *
 * @param {number} now - Current timestamp in milliseconds
 * @returns {number} Number of counters expired
 */
function expireCounters(now) {
    let done = false;
    let trimmed = 0;

    while (counters.size > 0 && !done) {
        const toRemove = [];

        // Scan counters in insertion order (oldest first)
        for (const [key, value] of counters.entries()) {
            // Expire if TAT is in the past (with 10s buffer to avoid churn)
            if (value <= now + 10000) {
                toRemove.push(key);

                // Limit batch size to 1000 for bounded latency
                if (toRemove.length >= 1000) {
                    break;
                }
            } else {
                // Map is ordered, so if we hit a future timestamp, we're done
                done = true;
                break;
            }
        }

        // Remove expired counters
        toRemove.forEach(k => counters.delete(k));
        trimmed += toRemove.length;

        // If we didn't find any expired counters, we're done
        if (toRemove.length === 0) {
            done = true;
        }
    }

    return trimmed;
}

/**
 * Set cached rate limit configuration
 *
 * Used by post-metadata checks to cache bucket rate limit config
 * for future pre-auth checks (avoids metadata load on every request).
 *
 * @param {string} key - Bucket name
 * @param {Object} limitConfig - Rate limit config {maxRps, burstCapacity}
 * @param {number} ttl - Time to live in milliseconds
 */
function setCachedConfig(key, limitConfig, ttl) {
    const expiry = Date.now() + ttl;
    configCache.set(key, { expiry, config: limitConfig });
}

/**
 * Get cached rate limit configuration
 *
 * Returns undefined if not cached or expired.
 *
 * @param {string} key - Bucket name
 * @returns {Object|undefined} Rate limit config or undefined
 */
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

module.exports = {
    setCounter,
    getCounter,
    expireCounters,
    setCachedConfig,
    getCachedConfig,

    // Do not access directly
    // Used only for tests
    counters,
    configCache,
};
