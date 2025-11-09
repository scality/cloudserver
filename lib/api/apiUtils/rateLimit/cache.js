/**
 * Rate Limit Counter Cache
 *
 * Stores GCRA counters in memory for zero-latency rate limiting decisions.
 *
 * COUNTER STRUCTURE (Iteration 2):
 *   Each counter stores:
 *   - tat: Theoretical Arrival Time (timestamp in ms when bucket empties)
 *   - interval: Time between requests in ms (for Redis sync)
 *   - lastSync: Timestamp of last Redis sync (for sync coordination)
 *
 * ORDERED MAP:
 *   Map maintains insertion order, so counters are naturally ordered by
 *   when they were last updated. This helps with efficient expiry.
 */

const counters = new Map();
const configCache = new Map();

/**
 * Set counter value with full metadata
 *
 * @param {string} key - Counter key (e.g., "throttling:bucket:my-bucket:rps")
 * @param {Object} counter - Counter object
 * @param {number} counter.tat - Theoretical Arrival Time (ms timestamp)
 * @param {number} counter.interval - Interval between requests (ms)
 * @param {number} counter.lastSync - Last Redis sync timestamp (ms)
 */
function setCounter(key, counter) {
    // Make sure that the Map remains in order
    // Counters expiring soonest will be first during iteration.
    counters.delete(key);
    counters.set(key, counter);
}

/**
 * Get counter value
 *
 * @param {string} key - Counter key
 * @returns {Object|undefined} Counter object or undefined if not found
 */
function getCounter(key) {
    return counters.get(key);
}

/**
 * Expire old counters using batch removal with early exit
 *
 * Map is ordered by insertion (counters expiring soonest are first).
 * Removes up to 1000 expired counters per call for bounded latency.
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
        for (const [key, counter] of counters.entries()) {
            // Expire if TAT is in the past (with 10s buffer to avoid churn)
            if (counter.tat <= now + 10000) {
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
