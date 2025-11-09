/**
 * Rate Limiting Enforcement Module
 *
 * Implements GCRA (Generic Cell Rate Algorithm) for distributed rate limiting across multiple CloudServer nodes.
 *
 * ============================================================================
 * ARCHITECTURE OVERVIEW
 * ============================================================================
 *
 * TWO-PHASE CHECKING:
 *   Phase 1 (Pre-Auth):  Check BEFORE Vault authentication using cached config (~0.1ms)
 *                        - Protects IAM service from excessive load
 *                        - Fail open if no cached config available
 *                        - Sets request.rateLimitPreAuthChecked = true if GCRA evaluated
 *
 *   Phase 2 (Post-Metadata): Check AFTER loading bucket metadata (~1ms)
 *                            - Authoritative check with fresh bucket config
 *                            - Populates cache for future pre-auth checks
 *                            - Skipped if Phase 1 already enforced rate limit
 *
 * IN-MEMORY FIRST DESIGN:
 *   - GCRA counters stored in-memory for zero-latency decisions
 *   - Background sync to local Redis every 10 seconds for persistence across workers
 *   - Dirty counter tracking: Only sync counters that changed since last cycle
 *
 * DISTRIBUTED QUOTA ALLOCATION:
 *   - Each node enforces R/N locally (where R = configured rate limit, N = node count)
 *   - Requires upstream load balancer with even traffic distribution
 *   - Trade-off: Simple implementation, but less fair if traffic is asymmetric
 *   - Future iterations will add cross-node synchronization
 *
 * FAIL-OPEN PHILOSOPHY:
 *   - Always allow requests if rate limiting components fail
 *   - Better to allow excess traffic than block legitimate requests
 *   - Errors are logged but never block the request path
 *
 * ============================================================================
 * GCRA ALGORITHM EXPLAINED
 * ============================================================================
 *
 * GCRA uses a "Theoretical Arrival Time" (TAT) which is a timestamp representing
 * when the rate limit "bucket" will be empty. Each request adds an interval
 * (1/RPS in milliseconds) to the TAT.
 *
 * EXAMPLE: 10 RPS limit means 100ms interval between requests
 *   - Counter (TAT) starts at 12:00:00.000
 *   - Request 1 at 12:00:00.000: TAT = 12:00:00.100 (allowed)
 *   - Request 2 at 12:00:00.050: TAT = 12:00:00.200 (allowed if burst >= 150ms)
 *   - Request 3 at 12:00:00.100: TAT = 12:00:00.300 (allowed if burst >= 200ms)
 *   - If TAT exceeds (now + burst capacity), request is rejected
 *
 * BURST CAPACITY:
 *   - Allows TAT to go into the future by up to burst capacity (in seconds)
 *   - Default: 1 second (allows brief traffic spikes)
 *   - burstCapacity=0: No burst, strict pacing (TAT must be <= now)
 *   - burstCapacity=5: Allow TAT up to 5 seconds in future (absorbs larger spikes)
 *
 * COUNTER RESET:
 *   - If TAT is in the past (bucket has emptied), reset to current time
 *   - This naturally handles idle periods without explicit cleanup
 *
 * WHY GCRA vs TOKEN BUCKET or LEAKY BUCKET:
 *   - Smoother traffic shaping (no "thundering herd" at window boundaries)
 *   - Single counter per bucket (simpler than token bucket refill logic)
 *   - Timestamp-based (no background refill threads needed)
 */

const { RateLimitClient } = require('./client');
const {
    getCounter,
    setCounter,
    expireCounters,
    getCachedConfig,
    setCachedConfig,
} = require('./cache');

// ============================================================================
// MODULE STATE
// ============================================================================

// Redis client for background sync between workers on the same node
let rateLimitClient = null;

// Dirty counter tracking: Maps counter keys to true for counters that have been
// modified since last Redis sync. Cleared after each sync cycle.
const dirtyCounters = new Map(); // key → true

// Throttling state tracking: Maps bucket names to true for buckets currently
// being throttled. Used to detect state transitions (throttled → allowed or
// allowed → throttled) for logging and Prometheus metrics.
const throttledBucketsState = new Map(); // bucketName → true

// Background worker intervals
const SYNC_INTERVAL = 10000; // Redis sync interval: 10 seconds
const EXPIRE_INTERVAL = 5000; // Counter expiry check interval: 5 seconds

// ============================================================================
// INITIALIZATION
// ============================================================================

/**
 * Initialize rate limiting system (Iteration 2)
 *
 * Called once during CloudServer startup. Sets up Redis client for worker
 * synchronization and starts two background workers:
 *   1. Counter sync worker (every 10s) - syncs dirty counters to Redis
 *   2. Counter expiry worker (every 5s) - removes old counters from memory
 *
 * ITERATION 2 CHANGES:
 *   - Redis client initialized for cross-worker state sharing
 *   - Background sync writes TAT values to Redis every 10 seconds
 *   - All workers on same node share GCRA state via local Redis
 *   - Provides counter persistence across worker restarts
 *
 * @param {Object} config - CloudServer configuration
 * @param {Object} logger - Logger instance
 */
function initRateLimit(config, logger) {
    // Fail gracefully if rate limiting is disabled
    if (!config.rateLimiting?.enabled) {
        logger.info('rate limiting disabled');
        return;
    }

    logger.info('initializing rate limiting', {
        nodeCount: config.rateLimiting.nodeCount || 1,
        defaultMaxRps: config.rateLimiting.default?.maxRps || 0,
        defaultBurstCapacity: config.rateLimiting.default?.burstCapacity || 0,
        redisSync: !!config.localCache,
    });

    // Initialize Redis client for background sync (if configured)
    if (config.localCache) {
        try {
            rateLimitClient = new RateLimitClient(config.localCache, logger);
            logger.info('Redis sync enabled for cross-worker fairness');
        } catch (err) {
            logger.warn('Redis client initialization failed - running without sync', {
                error: err.message,
            });
            // Continue without Redis (fail-open)
        }
    } else {
        logger.warn('localCache not configured - Redis sync disabled');
    }

    // Start background workers using setTimeout to avoid blocking startup
    // These will reschedule themselves after each run
    if (rateLimitClient) {
        setTimeout(() => syncCounters(config, logger), SYNC_INTERVAL);
    }
    setTimeout(() => expireOldCounters(logger), EXPIRE_INTERVAL);

    logger.info('rate limiting initialized');
}

// ============================================================================
// CORE GCRA ALGORITHM
// ============================================================================

/**
 * Format counter key for Redis/cache storage
 *
 * Counter key format: throttling:{scope}:{resource}:{metric}
 * Example: throttling:bucket:my-bucket:rps
 *
 * This format allows future expansion to:
 *   - throttling:account:123456789012:rps (account-level rate limiting)
 *   - throttling:bucket:my-bucket:bps (bandwidth/throughput limiting)
 *
 * @param {string} bucketName - Bucket name
 * @returns {string} Formatted Redis/cache key
 */
function formatCounterKey(bucketName) {
    return `throttling:bucket:${bucketName}:rps`;
}

/**
 * GCRA (Generic Cell Rate Algorithm) evaluation with burst capacity support
 *
 * This is the core rate limiting decision function. It maintains a "Theoretical
 * Arrival Time" (TAT) counter that represents when the rate limit "bucket" will
 * be empty. Each request adds an interval to the TAT.
 *
 * ALGORITHM STEPS:
 *   1. Get current TAT from counter (or initialize to now if empty/past)
 *   2. Calculate maximum allowed TAT = now + burst capacity
 *   3. If TAT > max allowed: REJECT (rate limit exceeded)
 *   4. Otherwise: ACCEPT and add interval to TAT
 *
 * BURST CAPACITY MECHANICS:
 *   - burstCapacity=0: Strict pacing, no burst (TAT must be <= now)
 *   - burstCapacity=1000ms: Allow TAT up to 1 second in future
 *   - This allows brief traffic spikes while enforcing average rate
 *
 * EXAMPLE with 10 RPS (100ms interval) and 1s burst:
 *   Request at 0ms:   TAT=0   + 100 = 100ms   (allowed, TAT 100ms ahead)
 *   Request at 50ms:  TAT=100 + 100 = 200ms   (allowed, TAT 150ms ahead)
 *   Request at 100ms: TAT=200 + 100 = 300ms   (allowed, TAT 200ms ahead)
 *   ...
 *   Request at 0ms:   TAT=1000 + 100 = 1100ms (allowed, TAT 1100ms ahead)
 *   Request at 0ms:   TAT=1100 > (0+1000)     (REJECTED, burst exhausted)
 *
 * @param {string} key - Counter key (from formatCounterKey)
 * @param {number} arrivedAt - Current timestamp in milliseconds (Date.now())
 * @param {number} interval - Interval between requests in milliseconds (1/RPS * 1000)
 * @param {number} burstCapacityMs - Burst capacity in milliseconds (default 0)
 * @returns {boolean} true if request allowed, false if throttled
 */
function evalGCRA(key, arrivedAt, interval, burstCapacityMs = 0) {
    // Special case: No rate limit configured (interval=0 means infinite RPS)
    if (interval === 0) {
        return true;
    }

    // Step 1: Get current counter from in-memory cache
    // Counter structure: { tat, interval, lastSync }
    let counter = getCounter(key);
    let emptyAt = counter ? counter.tat : null;

    // Step 2: If bucket is empty or TAT is in the past, reset to now
    // This naturally handles idle periods - after a pause, the counter resets
    // and the bucket starts "empty" again
    if (!emptyAt || emptyAt < arrivedAt) {
        emptyAt = arrivedAt;
    }

    // Step 3: Calculate maximum allowed TAT based on burst capacity
    // burstCapacity allows the TAT to go into the future by this amount
    // Example: arrivedAt=1000ms, burstCapacityMs=500ms => maxAllowedEmptyAt=1500ms
    const maxAllowedEmptyAt = arrivedAt + burstCapacityMs;

    // Step 4: Check if request would exceed burst capacity
    // If TAT is already beyond max allowed, the bucket is "full" - reject request
    if (emptyAt > maxAllowedEmptyAt) {
        return false; // REJECT - burst capacity exhausted
    }

    // Step 5: Accept request and update TAT by adding interval
    // This "fills" the bucket by one request worth of time
    const newEmptyAt = emptyAt + interval;

    // Step 6: Update counter with new TAT and mark as dirty for Redis sync
    setCounter(key, {
        tat: newEmptyAt,
        interval,
        lastSync: counter ? counter.lastSync : Date.now(),
    });

    // Mark counter as dirty so background worker syncs it to Redis
    dirtyCounters.set(key, true);

    return true; // ALLOW - request within rate limit
}

// ============================================================================
// PHASE 2: POST-METADATA RATE LIMIT CHECK
// ============================================================================

/**
 * Check if bucket rate limit allows the request (Phase 2 - Post-Metadata)
 *
 * This is the AUTHORITATIVE rate limit enforcement function called by API handlers
 * AFTER loading bucket metadata. It has access to the bucket's actual rate limit
 * configuration and populates the cache for future pre-auth checks.
 *
 * WHEN CALLED:
 *   - Called from standardMetadataValidateBucketAndObj and standardMetadataValidateBucket
 *   - Runs AFTER Vault authentication and bucket metadata load
 *   - Skipped if request.rateLimitPreAuthChecked is true (pre-auth already enforced)
 *
 * CONFIGURATION PRIORITY:
 *   1. Bucket-specific rate limit (from bucket.getRateLimitConfiguration())
 *   2. Global default rate limit (from config.rateLimiting.default.maxRps)
 *   3. No rate limit (maxRps=0)
 *
 * DISTRIBUTED RATE CALCULATION:
 *   Given R (configured rate) and N (node count):
 *     perNodeRps = R / N
 *     interval = (1 / perNodeRps) * 1000 milliseconds
 *   Example: 1000 RPS across 10 nodes = 100 RPS per node = 10ms interval
 *
 * CACHE POPULATION:
 *   After determining the rate limit, this function caches the config with a TTL
 *   (default 30s) so future requests can use pre-auth checking for lower latency.
 *
 * @param {string} bucketName - Bucket name
 * @param {Object} bucket - BucketInfo object with rate limit configuration
 * @param {Object} config - CloudServer configuration
 * @param {Object} log - Logger instance
 * @returns {boolean} true if request allowed, false if throttled
 */
function checkBucketRateLimit(bucketName, bucket, config, log) {
    // Early exit: Rate limiting disabled globally
    if (!config.rateLimiting?.enabled) {
        return true;
    }

    // Step 1: Determine rate limit configuration
    // Priority: bucket config > default config > no limit
    let maxRps = 0;
    let burstCapacity = config.rateLimiting.default?.burstCapacity ?? 1; // Default 1 second
    // Note: Use ?? instead of || to allow burstCapacity=0 (no burst)

    const rateLimitConfig = bucket.getRateLimitConfiguration();

    if (rateLimitConfig && rateLimitConfig.getRequestsPerSecondLimit() !== undefined) {
        // Bucket has explicit rate limit configured via PUT /{bucket}?rate-limit API
        maxRps = rateLimitConfig.getRequestsPerSecondLimit();
        // TODO: Get burstCapacity from bucket config when BucketInfo model supports it
        // burstCapacity = rateLimitConfig.getBurstCapacity() ?? burstCapacity;
    } else if (config.rateLimiting.default?.maxRps !== undefined) {
        // Use global default rate limit from config
        maxRps = config.rateLimiting.default.maxRps;
    }

    // Early exit: No rate limit configured for this bucket
    if (maxRps === 0) {
        return true;
    }

    // Step 2: Populate cache for future pre-auth checks
    // This allows subsequent requests to be checked before Vault authentication
    const ttl = config.rateLimiting.bucket?.configCacheTTL || 30000; // Default 30s
    setCachedConfig(bucketName, { maxRps, burstCapacity }, ttl);

    // Step 3: Calculate per-node rate (distributed quota allocation)
    // Each CloudServer node enforces R/N locally where:
    //   R = configured rate limit
    //   N = total number of CloudServer nodes
    const nodeCount = config.rateLimiting.nodeCount || 1;
    const perNodeRps = maxRps / nodeCount;

    // Warn if per-node rate is very low (< 1 RPS)
    // This indicates the rate limit might be too low for the number of nodes
    if (perNodeRps < 1) {
        log.warn('per-node rate limit < 1 RPS', {
            bucketName,
            maxRps,
            nodeCount,
            perNodeRps,
        });
    }

    // Step 4: Calculate GCRA parameters
    // Interval = time between requests in milliseconds
    // Example: 10 RPS => interval = 1/10 * 1000 = 100ms
    const interval = perNodeRps > 0 ? (1 / perNodeRps) * 1000 : 0;

    // Convert burst capacity from seconds to milliseconds
    const burstCapacityMs = burstCapacity * 1000;

    // Step 5: Evaluate GCRA algorithm
    const key = formatCounterKey(bucketName);
    const now = Date.now();
    const allowed = evalGCRA(key, now, interval, burstCapacityMs);

    // TODO: REMOVE DEBUG LOGGING (temporary for development)
    if (!allowed) {
        log.debug('GCRA rejected', {
            bucketName,
            maxRps,
            interval,
            burstCapacity,
            burstCapacityMs,
        });
    }
    // END DEBUG

    // Step 6: Track throttling state transitions for monitoring/metrics
    // We only log and update Prometheus when state changes (not on every throttled request)
    const wasThrottled = throttledBucketsState.has(bucketName);

    if (!allowed) {
        // Request throttled - check if this is a new throttling event
        if (!wasThrottled) {
            // State transition: allowed → throttled
            throttledBucketsState.set(bucketName, true);
            log.debug('bucket throttled', { bucketName, maxRps, perNodeRps });

            // Update Prometheus metric (fail gracefully if monitoring unavailable)
            try {
                const monitoring = require('../../utilities/monitoringHandler');
                monitoring.throttledBuckets.set({ bucket: bucketName }, 1);
            } catch (err) {
                // Ignore monitoring errors - never fail rate limiting due to metrics
            }
        }
    } else if (wasThrottled) {
        // Request allowed - check if bucket recovered from throttling
        // State transition: throttled → allowed
        throttledBucketsState.delete(bucketName);
        log.debug('bucket throttling cleared', { bucketName });

        // Update Prometheus metric (fail gracefully if monitoring unavailable)
        try {
            const monitoring = require('../../utilities/monitoringHandler');
            monitoring.throttledBuckets.set({ bucket: bucketName }, 0);
        } catch (err) {
            // Ignore monitoring errors
        }
    }

    return allowed;
}

// ============================================================================
// PHASE 1: PRE-AUTHENTICATION RATE LIMIT CHECK
// ============================================================================

/**
 * Pre-authentication rate limit check using cached configuration (Phase 1 - Pre-Auth)
 *
 * This function is called BEFORE Vault authentication to provide early rejection
 * of excessive requests, protecting the IAM service and metadata backend from load.
 *
 * WHEN CALLED:
 *   - Called from api.js BEFORE Vault authentication
 *   - Runs at the very start of request processing (~0.1ms)
 *   - Uses CACHED rate limit config from previous post-metadata checks
 *
 * FAIL-OPEN BEHAVIOR:
 *   Returns { checked: false, allowed: true } in these cases:
 *   - Rate limiting globally disabled
 *   - No cached config available (first request to this bucket)
 *   - Cached config shows maxRps=0 (no rate limit)
 *
 *   When failing open, request proceeds to Phase 2 (post-metadata) check which
 *   will load fresh config and populate the cache.
 *
 * CACHE DEPENDENCIES:
 *   - Cache is populated by checkBucketRateLimit() (Phase 2) after loading bucket metadata
 *   - Cache has TTL (default 30s) to avoid stale config
 *   - Cache may not exist for buckets that haven't been accessed recently
 *
 * RETURN VALUE:
 *   { checked: boolean, allowed: boolean }
 *   - checked=true:  GCRA was evaluated (cache hit)
 *   - checked=false: GCRA not evaluated (cache miss or disabled)
 *   - allowed=true:  Request allowed (either cache miss or within rate limit)
 *   - allowed=false: Request throttled (cache hit and rate limit exceeded)
 *
 * @param {string} bucketName - Bucket name
 * @param {Object} config - CloudServer configuration
 * @param {Object} log - Logger instance
 * @returns {Object} { checked: boolean, allowed: boolean }
 */
function rateLimitPreCheck(bucketName, config, log) {
    // Early exit: Rate limiting disabled globally
    if (!config.rateLimiting?.enabled) {
        return { checked: false, allowed: true };
    }

    // Step 1: Check if we have cached config for this bucket
    const cachedConfig = getCachedConfig(bucketName);
    if (!cachedConfig) {
        // Cache miss - fail open, allow request to proceed to Phase 2
        // Phase 2 will load bucket metadata and populate the cache
        return { checked: false, allowed: true };
    }

    // Step 2: Extract rate limit from cache
    const { maxRps, burstCapacity } = cachedConfig;
    if (maxRps === 0 || maxRps === undefined) {
        // Cached config shows no rate limit for this bucket
        return { checked: false, allowed: true };
    }

    // Step 3: Calculate GCRA parameters (same logic as Phase 2)
    // Per-node rate allocation: R/N
    const nodeCount = config.rateLimiting.nodeCount || 1;
    const perNodeRps = maxRps / nodeCount;
    const interval = perNodeRps > 0 ? (1 / perNodeRps) * 1000 : 0;

    // Convert burst capacity from seconds to milliseconds
    const burstCapacitySec = burstCapacity ?? 1; // Default 1 second
    const burstCapacityMs = burstCapacitySec * 1000;

    // Step 4: Evaluate GCRA using cached configuration
    const key = formatCounterKey(bucketName);
    const now = Date.now();
    const allowed = evalGCRA(key, now, interval, burstCapacityMs);

    if (!allowed) {
        log.debug('request throttled (pre-auth cache hit)', {
            bucketName,
            maxRps,
            perNodeRps,
            cacheHit: true,
        });
    }

    // Return checked=true to indicate GCRA was evaluated
    // If allowed=false, Phase 2 will be skipped (request already throttled)
    return { checked: true, allowed };
}

// ============================================================================
// BACKGROUND WORKERS
// ============================================================================

/**
 * Background worker: Sync dirty counters to Redis (Iteration 2)
 *
 * Runs every 10 seconds to persist in-memory GCRA counters to local Redis.
 * This provides cross-worker state sharing and persistence across restarts.
 *
 * ARCHITECTURE:
 *   - 10 Node.js workers per physical node share 1 local Redis
 *   - Only dirty (modified) counters are synced (performance optimization)
 *   - Lua script ensures only newer TAT values overwrite Redis
 *   - Fail-open: If Redis fails, workers continue with independent counters
 *
 * SYNC PROCESS:
 *   1. Snapshot dirty counter keys and clear dirty map
 *   2. Get current TAT values from in-memory cache
 *   3. Batch sync to Redis using pipeline
 *   4. Log errors but don't block request path
 *
 * @param {Object} config - CloudServer configuration
 * @param {Object} logger - Logger instance
 */
function syncCounters(config, logger) {
    try {
        // Early exit: Redis not configured or initialization failed
        if (!rateLimitClient) {
            return;
        }

        // Step 1: Snapshot dirty counters and clear for next cycle
        // This ensures each counter is synced once even if modified during sync
        const toSync = Array.from(dirtyCounters.keys());
        dirtyCounters.clear();

        // Early exit: No dirty counters, nothing to sync
        if (toSync.length === 0) {
            return;
        }

        logger.debug('syncing counters to Redis', { count: toSync.length });

        // Step 2: Build batch of counters to sync
        const batch = toSync.map(key => {
            const counter = getCounter(key);
            return counter ? { key, tat: counter.tat } : null;
        }).filter(item => item !== null);

        // Step 3: Sync to Redis using batch operation
        if (batch.length > 0) {
            rateLimitClient.syncCounters(batch, (err, results) => {
                if (err) {
                    logger.error('Redis sync error', {
                        error: err.message,
                        count: batch.length,
                    });
                    // Fail-open: Continue despite error
                } else {
                    logger.debug('counters synced to Redis', {
                        count: batch.length,
                    });

                    // Optional: Update lastSync timestamp for synced counters
                    const now = Date.now();
                    results.forEach(({ key }) => {
                        const counter = getCounter(key);
                        if (counter) {
                            counter.lastSync = now;
                        }
                    });
                }
            });
        }
    } catch (err) {
        // Fail-open: Log error but don't crash worker or block requests
        logger.error('failed to sync counters to Redis', {
            error: err.message,
            stack: err.stack,
        });
        // In-memory rate limiting continues to work despite Redis failure
    } finally {
        // Always reschedule next sync (runs indefinitely)
        setTimeout(() => syncCounters(config, logger), SYNC_INTERVAL);
    }
}

/**
 * Background worker: Remove expired counters from memory
 *
 * Runs every 5 seconds to clean up old GCRA counters that are no longer active.
 * This prevents memory leaks when buckets have rate limits but low traffic.
 *
 * EXPIRY LOGIC:
 *   - Counters are expired when TAT (emptyAt) is sufficiently in the past
 *   - Implemented in cache.js using Map iteration with early exit
 *   - Expiry threshold includes 10 second buffer to avoid premature cleanup
 *
 * WHY EXPIRY IS NEEDED:
 *   - GCRA counters don't automatically clean up after idle periods
 *   - Without expiry, memory would grow indefinitely as new buckets are accessed
 *   - Target: Support 20,000 active buckets within 1,000,000 total buckets
 *
 * PERFORMANCE:
 *   - Map iteration allows O(k) expiry where k = expired counters (not O(n))
 *   - Batched removal (max 1000 per cycle) for bounded latency
 *   - No impact on hot path (request processing)
 *   - Runs asynchronously in background
 *
 * @param {Object} logger - Logger instance
 */
function expireOldCounters(logger) {
    try {
        const now = Date.now();
        // Delegate to cache module which maintains Map for efficient expiry
        expireCounters(now);
    } catch (err) {
        // Fail-open: Log error but don't crash worker
        logger.error('failed to expire counters', {
            error: err.message,
        });
        // Worst case: Memory usage grows until next successful expiry
    } finally {
        // Always reschedule next expiry (runs indefinitely)
        setTimeout(() => expireOldCounters(logger), EXPIRE_INTERVAL);
    }
}

// ============================================================================
// MONITORING / METRICS
// ============================================================================

/**
 * Get currently throttled buckets (for monitoring/metrics)
 *
 * Returns a Map of bucket names that are currently being throttled. This is used
 * for Prometheus metrics and operational dashboards.
 *
 * State transitions are tracked in checkBucketRateLimit():
 *   - allowed → throttled: Bucket added to map, metric set to 1
 *   - throttled → allowed: Bucket removed from map, metric set to 0
 *
 * @returns {Map<string, boolean>} Map of throttled bucket names
 */
function getThrottledBuckets() {
    return throttledBucketsState;
}

// ============================================================================
// MODULE EXPORTS
// ============================================================================

module.exports = {
    // Lifecycle
    initRateLimit,

    // Two-phase enforcement
    rateLimitPreCheck,       // Phase 1: Pre-auth check using cached config
    checkBucketRateLimit,    // Phase 2: Post-metadata check (also populates cache)

    // Monitoring
    getThrottledBuckets,     // For Prometheus metrics

    // Testing only
    evalGCRA,                // Core GCRA algorithm
    formatCounterKey,        // Counter key formatting
};
