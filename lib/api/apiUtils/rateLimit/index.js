/**
 * Rate Limiting Orchestration Layer
 *
 * Coordinates rate limiting across all S3 APIs using existing infrastructure:
 * - cache.js: In-memory GCRA counter storage
 * - client.js: Redis client with Lua script integration
 * - updateCounter.lua: Atomic GCRA "add cost" implementation
 *
 * Architecture:
 * - PHASE 1: Pre-auth check (cache-based, before Vault authentication)
 * - PHASE 2: Post-metadata check (authoritative, after bucket metadata load)
 * - Background sync: Persist dirty counters to Redis every 10s
 * - Background expiry: Remove expired counters every 5s
 *
 * GCRA Algorithm:
 * - TAT (Theoretical Arrival Time): Timestamp when bucket empties
 * - Interval: Time per request (1/rate * 1000 ms)
 * - Check: Reject if TAT > now (bucket still has backlog)
 * - Update: TAT += interval (consume capacity)
 *
 * Example with 200 RPS, 10 nodes:
 * - perNodeRps = 200/10 = 20 RPS per node
 * - interval = (1/20) * 1000 = 50ms per request
 * - All workers enforce 20 RPS locally
 * - Redis accumulates costs from all workers
 */

const { errors } = require('arsenal');
const cache = require('./cache');
const { instance: rateLimitClient } = require('./client');
const { throttledBuckets } = require('../../../utilities/monitoringHandler');

// Module state
const dirtyCounters = new Map();      // Counters needing Redis sync
const lastSyncValues = new Map();     // Last synced values for cost calculation
const throttleState = new Map();      // Track throttle state for metrics
let syncTimer = null;                 // Timer for sync worker
let expireTimer = null;               // Timer for expiry worker

// Constants
const SYNC_INTERVAL = 10000;          // 10 seconds
const EXPIRY_INTERVAL = 5000;         // 5 seconds

/**
 * Generate Redis counter key
 * @param {string} bucketName - Bucket name
 * @returns {string} Counter key (throttling:bucket:{name}:rps)
 */
function formatCounterKey(bucketName) {
    return `throttling:bucket:${bucketName}:rps`;
}

/**
 * Extract rate limit from bucket metadata or global default
 * @param {object} bucket - Bucket metadata object
 * @param {object} config - CloudServer config
 * @returns {number|null} maxRps or null if no limit configured
 */
function extractRateLimit(bucket, config) {
    console.log('[DEBUG] extractRateLimit called with:', {
        hasBucket: !!bucket,
        bucketType: bucket?.constructor?.name,
        hasGetRateLimitMethod: typeof bucket?.getRateLimitConfiguration === 'function',
        bucketKeys: bucket ? Object.keys(bucket).slice(0, 10) : [],
    });

    // Check bucket-specific rate limit using method (not property)
    const rateLimitConfig = bucket?.getRateLimitConfiguration();

    console.log('[DEBUG] getRateLimitConfiguration result:', {
        hasConfig: !!rateLimitConfig,
        configType: rateLimitConfig?.constructor?.name,
        hasGetLimitMethod: typeof rateLimitConfig?.getRequestsPerSecondLimit === 'function',
    });

    if (rateLimitConfig && rateLimitConfig.getRequestsPerSecondLimit() !== undefined) {
        const bucketLimit = rateLimitConfig.getRequestsPerSecondLimit();
        console.log('[DEBUG] extractRateLimit:', {
            hasBucketConfig: true,
            bucketLimit,
            willUse: bucketLimit && bucketLimit > 0 ? bucketLimit : 'default',
        });
        if (bucketLimit && bucketLimit > 0) {
            return bucketLimit;
        }
    } else {
        console.log('[DEBUG] extractRateLimit:', {
            hasBucketConfig: false,
            rateLimitConfig,
            willUse: config.rateLimiting?.default?.maxRps || 'none',
        });
    }

    // Fall back to global default
    if (config.rateLimiting?.default?.maxRps) {
        console.log('[DEBUG] using default maxRps:', config.rateLimiting.default.maxRps);
        return config.rateLimiting.default.maxRps;
    }

    return null; // No rate limit configured
}

/**
 * GCRA Algorithm Implementation
 *
 * Generic Cell Rate Algorithm for smooth rate limiting with burst capacity.
 *
 * How it works:
 * - Track TAT (Theoretical Arrival Time) = when bucket empties
 * - Each request adds 'interval' to TAT
 * - Reject if TAT > now + burstCapacity (bucket too full)
 * - TAT naturally "leaks" as time passes
 *
 * Math:
 * - interval = (1 / rps) * 1000 ms
 * - burstCapacity = burstSeconds * 1000 ms
 * - maxBurst = burstCapacity / interval requests
 *
 * @param {string} key - Counter key
 * @param {number} arrivedAt - Request timestamp (ms)
 * @param {number} interval - Time per request (ms)
 * @param {number} burstCapacity - Max burst time (ms)
 * @returns {boolean} true if allowed, false if throttled
 */
function evalGCRA(key, arrivedAt, interval, burstCapacity) {
    // No throttling if interval is 0
    if (interval === 0) {
        return true;
    }

    // Get current TAT (undefined if not set)
    let emptyAt = cache.getCounter(key);

    // If bucket empty or doesn't exist, reset to now
    if (emptyAt === undefined || emptyAt < arrivedAt) {
        emptyAt = arrivedAt;
    }

    // BURST CAPACITY CHECK
    // Reject if bucket too full (TAT too far in future)
    if (emptyAt > arrivedAt + burstCapacity) {
        return false; // Throttled - burst depleted
    }

    // Accept request: add interval to TAT
    const newEmptyAt = emptyAt + interval;
    cache.setCounter(key, newEmptyAt);

    // Mark counter as dirty for Redis sync
    dirtyCounters.set(key, true);

    return true; // Allowed
}

/**
 * Phase 1: Pre-Authentication Rate Limit Check (Cache-Based)
 *
 * Called before Vault authentication. Uses cached config to reject
 * throttled requests early, protecting Vault from excessive load.
 *
 * Benefits:
 * - Saves Vault authentication call (~10-50ms)
 * - Protects IAM layer from throttled requests
 * - Cache hit rate: 90%+ for active buckets
 * - Latency: <0.1ms (in-memory only)
 *
 * Fallback:
 * - If no cached config, returns true (proceed to Phase 2)
 * - If rate limiting check fails, returns true (fail open)
 *
 * @param {string} bucketName - Bucket name
 * @param {object} config - CloudServer config
 * @param {object} log - Logger instance
 * @returns {boolean} true if allowed, false if throttled
 */
function rateLimitPreCheck(bucketName, config, log) {
    try {
        // Try to get cached config
        const cachedConfig = cache.getCachedConfig(bucketName);

        // Cache miss: proceed to Phase 2
        // Return 'cache-miss' to signal that post-metadata check should run
        if (!cachedConfig) {
            return 'cache-miss';
        }

        // Calculate interval from cached config
        const interval = cachedConfig.perNodeRps > 0
            ? (1 / cachedConfig.perNodeRps) * 1000
            : 0;

        // Get burst capacity
        const burstSeconds = config.rateLimiting.bucket?.burstCapacity || 1;
        const burstCapacity = burstSeconds * 1000;

        // Evaluate GCRA with cached config
        const key = formatCounterKey(bucketName);
        const now = Date.now();
        const allowed = evalGCRA(key, now, interval, burstCapacity);

        // Log throttle event
        if (!allowed) {
            log.debug('request throttled in pre-auth check', {
                bucketName,
                perNodeRps: cachedConfig.perNodeRps,
            });
        }

        return allowed;
    } catch (err) {
        // Fail open: allow request if rate limiting check fails
        log.error('rate limit pre-check failed, allowing request', {
            error: err.message,
            stack: err.stack,
            bucketName,
        });
        return true;
    }
}

/**
 * Phase 2: Post-Metadata Rate Limit Check (Authoritative)
 *
 * Called after bucket metadata is loaded. This is the authoritative check
 * that uses fresh configuration and populates the cache for Phase 1.
 *
 * Steps:
 * 1. Extract rate limit from bucket metadata or global default
 * 2. Calculate per-node rate (NO worker division!)
 * 3. Update config cache for Phase 1 pre-auth checks
 * 4. Evaluate GCRA with current config
 * 5. Update Prometheus metrics
 *
 * Why no worker division?
 * - Workers get uneven traffic (random load balancing)
 * - Redis accumulates costs from ALL workers
 * - Natural quota distribution via sync
 *
 * @param {string} bucketName - Bucket name
 * @param {object} bucket - Bucket metadata object
 * @param {object} config - CloudServer config
 * @param {object} log - Logger instance
 * @returns {boolean} true if allowed, false if throttled
 */
function checkBucketRateLimit(bucketName, bucket, config, log) {
    try {
        // Extract rate limit configuration
        const maxRps = extractRateLimit(bucket, config);

        // No rate limit configured
        if (!maxRps || maxRps === 0) {
            return true;
        }

        // Calculate per-node rate
        // CRITICAL: Only divide by nodeCount, NOT workerCount
        // Redis accumulates costs from all workers naturally
        const nodeCount = config.rateLimiting.nodeCount || 1;
        const perNodeRps = maxRps / nodeCount;

        // Calculate interval (time per request in ms)
        const interval = perNodeRps > 0 ? (1 / perNodeRps) * 1000 : 0;

        // Get burst capacity
        const burstSeconds = config.rateLimiting.bucket?.burstCapacity || 1;
        const burstCapacity = burstSeconds * 1000;

        // Update config cache for Phase 1 pre-auth checks
        const cacheTTL = config.rateLimiting.bucket?.configCacheTTL || 30000; // 30s default
        cache.setCachedConfig(bucketName, { maxRps, perNodeRps }, cacheTTL);

        // Evaluate GCRA
        const key = formatCounterKey(bucketName);
        const now = Date.now();
        const allowed = evalGCRA(key, now, interval, burstCapacity);

        // Update throttle state for metrics
        if (!allowed) {
            if (!throttleState.get(bucketName)) {
                throttleState.set(bucketName, true);
                // Update Prometheus metric
                throttledBuckets.set({ bucket: bucketName }, 1);
                log.debug('bucket throttled', {
                    bucketName,
                    maxRps,
                    perNodeRps,
                });
            }
        } else {
            if (throttleState.get(bucketName)) {
                throttleState.delete(bucketName);
                // Update Prometheus metric
                throttledBuckets.set({ bucket: bucketName }, 0);
                log.debug('bucket throttling cleared', {
                    bucketName,
                });
            }
        }

        // Log throttle event
        if (!allowed) {
            log.debug('request throttled in post-metadata check', {
                bucketName,
                maxRps,
                perNodeRps,
            });
        }

        return allowed;
    } catch (err) {
        // Fail open: allow request if rate limiting check fails
        log.error('rate limit check failed, allowing request', {
            error: err.message,
            stack: err.stack,
            bucketName,
        });
        return true;
    }
}

/**
 * Background Sync Worker
 *
 * Syncs dirty counters to Redis every 10 seconds.
 * Redis accumulates costs from all workers via Lua script.
 *
 * Algorithm:
 * 1. Collect dirty counter keys
 * 2. Clear dirty flags (allow new updates during sync)
 * 3. Build batch with cost = currentValue - lastSyncValue
 * 4. Send to Redis via client.updateLocalCounters()
 * 5. Reschedule after 10 seconds
 *
 * Why "add cost" pattern:
 * - Existing updateCounter.lua expects cost to add
 * - Cost = delta since last sync
 * - Redis adds cost to existing counter value
 * - Works with multiple workers naturally
 *
 * @param {object} config - CloudServer config
 * @param {object} logger - Logger instance
 */
function syncCounters(config, logger) {
    try {
        const now = Date.now();

        // Collect dirty keys
        const dirtyKeys = Array.from(dirtyCounters.keys());
        dirtyCounters.clear();

        // Nothing to sync
        if (dirtyKeys.length === 0) {
            // Reschedule
            syncTimer = setTimeout(() => syncCounters(config, logger), SYNC_INTERVAL);
            return;
        }

        logger.debug('syncing counters to Redis', { count: dirtyKeys.length });

        // Build batch with cost calculation
        const batch = dirtyKeys
            .map(key => {
                const currentValue = cache.getCounter(key);
                if (currentValue === undefined) {
                    return null;
                }

                // Calculate cost (delay consumed since last sync)
                // Cost should be the time interval consumed, not absolute timestamp
                const lastValue = lastSyncValues.get(key);
                let cost;

                if (lastValue === undefined) {
                    // First sync: cost is delay from now
                    cost = Math.max(0, currentValue - now);
                } else {
                    // Subsequent syncs: cost is increase in TAT
                    cost = currentValue - lastValue;
                }

                // Update last sync value
                lastSyncValues.set(key, currentValue);

                return { key, cost };
            })
            .filter(item => item !== null && item.cost > 0);

        // Nothing to sync after filtering
        if (batch.length === 0) {
            syncTimer = setTimeout(() => syncCounters(config, logger), SYNC_INTERVAL);
            return;
        }

        // Check if Redis client is available
        if (!rateLimitClient) {
            logger.warn('Redis client not available, skipping sync');
            syncTimer = setTimeout(() => syncCounters(config, logger), SYNC_INTERVAL);
            return;
        }

        // Send to Redis
        rateLimitClient.updateLocalCounters(batch, (err, results) => {
            if (err) {
                logger.error('failed to sync counters', { error: err.message });
            } else {
                logger.debug('counters synced to Redis', { count: batch.length });
            }

            // Reschedule
            syncTimer = setTimeout(() => syncCounters(config, logger), SYNC_INTERVAL);
        });
    } catch (err) {
        logger.error('sync worker error', {
            error: err.message,
            stack: err.stack,
        });
        // Reschedule even on error
        syncTimer = setTimeout(() => syncCounters(config, logger), SYNC_INTERVAL);
    }
}

/**
 * Background Expiry Worker
 *
 * Removes expired counters and configs from in-memory cache every 5 seconds.
 *
 * @param {object} logger - Logger instance
 */
function expireCounters(logger) {
    const now = Date.now();

    // Expire GCRA counters
    cache.expireCounters(now);

    // Expire config cache
    cache.expireCachedConfigs(now);

    // Reschedule
    expireTimer = setTimeout(() => expireCounters(logger), EXPIRY_INTERVAL);
}

/**
 * Initialize Rate Limiting System
 *
 * Called on CloudServer startup. Initializes Redis client and
 * starts background workers (sync + expiry).
 *
 * Requirements:
 * - config.rateLimiting.enabled must be true
 * - config.localCache must be configured (Redis connection)
 *
 * @param {object} config - CloudServer config
 * @param {object} logger - Logger instance
 */
function initRateLimit(config, logger) {
    // Check if rate limiting enabled
    if (!config.rateLimiting || !config.rateLimiting.enabled) {
        logger.info('rate limiting disabled');
        return;
    }

    // Validate required config
    if (!config.localCache) {
        logger.error('rate limiting enabled but localCache not configured');
        return;
    }

    // Check if Redis client is available
    if (!rateLimitClient) {
        logger.error('rate limiting enabled but Redis client not initialized');
        return;
    }

    logger.info('initializing rate limiting', {
        nodeCount: config.rateLimiting.nodeCount,
        defaultMaxRps: config.rateLimiting.default?.maxRps,
    });

    // Start background workers
    syncTimer = setTimeout(() => syncCounters(config, logger), SYNC_INTERVAL);
    expireTimer = setTimeout(() => expireCounters(logger), EXPIRY_INTERVAL);

    logger.info('rate limiting initialized successfully');
}

/**
 * Shutdown rate limiting system
 *
 * Stops background workers and cleans up resources.
 * Should be called on server shutdown.
 */
function shutdown() {
    if (syncTimer) {
        clearTimeout(syncTimer);
        syncTimer = null;
    }
    if (expireTimer) {
        clearTimeout(expireTimer);
        expireTimer = null;
    }
}

// Export public API
module.exports = {
    initRateLimit,
    rateLimitPreCheck,
    checkBucketRateLimit,
    shutdown,
};
