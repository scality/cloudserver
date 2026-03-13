/**
 * WorkerTokenBucket - Local token buffer for rate limiting
 *
 * Each worker maintains a local token bucket that holds tokens granted by Redis.
 * Tokens are consumed synchronously (in-memory) for each request, and refilled
 * asynchronously in the background.
 *
 * This design keeps Redis OUT of the hot path:
 * - Request handling: Pure in-memory token consumption (fast)
 * - Token refills: Async background requests to Redis (no blocking)
 */

const util = require('util');

const { instance: redisClient } = require('./client');
const { config } = require('../../../Config');
const { calculateInterval } = require('./gcra');

// Map of `${resourceClass}:${resourceId}:${measure}` -> WorkerTokenBucket instance
const tokenBuckets = new Map();

/**
 * Per-resourceClass+resourceID+measure token bucket for a single worker
 */
class WorkerTokenBucket {
    constructor(resourceClass, resourceId, measure, limitConfig, log) {
        this.resourceClass = resourceClass;
        this.resourceId = resourceId;
        this.measure = measure;
        this.limitConfig = limitConfig;
        this.log = log;

        this.bufferSize = config.rateLimiting?.tokenBucketBufferSize; // Max tokens to hold
        this.refillThreshold = config.rateLimiting?.tokenBucketRefillThreshold; // Trigger refill when below this
        this.tokens = this.bufferSize; // Start with full buffer for fail-open at startup
        this.interval = calculateInterval(this.limitConfig.limit, config.rateLimiting.nodes);
        this.lastRefillTime = Date.now();
        this.refillCount = 0;
        this.refillInProgress = false;
    }

    hasCapacity() {
        return this.tokens > 0;
    }

    updateLimit(updatedConfig) {
        if (this.limitConfig.limit !== updatedConfig.limit ||
            this.limitConfig.burstCapacity !== updatedConfig.burstCapacity
        ) {
            const oldConfig = this.limitConfig;
            this.limitConfig = updatedConfig;
            this.interval = calculateInterval(updatedConfig.limit, config.rateLimiting.nodes);
            return { updated: true, oldConfig };
        }

        return { updated: false };
    }

    /**
     * Try to consume one token (hot path - in-memory only)
     *
     * @returns {boolean} True if request allowed, false if throttled
     */
    tryConsume() {
        if (this.tokens > 0) {
            this.tokens -= 1;
            return true; // ALLOWED
        }

        return false; // THROTTLED
    }

    /**
     * Check if refill is needed and trigger async refill
     * Called by background job every 100ms
     *
     * @returns {Promise<void>}
     */
    async refillIfNeeded() {
        // Already refilling, skip
        if (this.refillInProgress) {
            return;
        }

        // Above threshold, no need to refill yet
        if (this.tokens >= this.refillThreshold) {
            return;
        }

        this.refillInProgress = true;
        const startTime = Date.now();

        try {
            // Calculate how many tokens to request
            const requested = this.bufferSize - this.tokens;

            if (requested <= 0) {
                return; // Buffer already full
            }

            // Calculate GCRA parameters
            let granted = requested;
            if (redisClient.isReady()) {
                // Request tokens from Redis (atomic GCRA enforcement)
                granted = await util.promisify(redisClient.grantTokens.bind(redisClient))(
                    this.resourceClass,
                    this.resourceId,
                    this.measure,
                    requested,
                    this.interval,
                    this.limitConfig.burstCapacity,
                );
            } else {
                // Connection to redis has failed in some way.
                // Client will be reconnecting in the background.
                // We grant the requested amount of tokens anyway to avoid degrading service availability.
                this.log.warn(
                    'rate limit redis client not connected. granting tokens anyway to avoid service degradation',
                    {
                        resourceClass: this.resourceClass,
                        resourceId: this.resourceId,
                        measure: this.measure,
                    },
                );
            }

            // Add granted tokens to buffer
            this.tokens += granted;
            this.refillCount++;

            this.lastRefillTime = Date.now();
            const duration = this.lastRefillTime - startTime;

            this.log.debug('Token refill completed', {
                resourceClass: this.resourceClass,
                resourceId: this.resourceId,
                measure: this.measure,
                requested,
                granted,
                newBalance: this.tokens,
                durationMs: duration,
                refillCount: this.refillCount,
            });

            // Warn if refill took too long or granted too few
            if (duration > 100) {
                this.log.warn('Slow token refill detected', {
                    resourceClass: this.resourceClass,
                    resourceId: this.resourceId,
                    measure: this.measure,
                    durationMs: duration,
                });
            }

            if (granted === 0 && requested > 0) {
                this.log.trace('Token refill denied - quota exhausted', {
                    resourceClass: this.resourceClass,
                    resourceId: this.resourceId,
                    measure: this.measure,
                    requested,
                });
            }

        } catch (err) {
            this.log.error('Token refill failed', {
                resourceClass: this.resourceClass,
                resourceId: this.resourceId,
                measure: this.measure,
                error: err.message,
                stack: err.stack,
            });
        } finally {
            this.refillInProgress = false;
        }
    }
}

/**
 * Get or create token bucket for a bucket
 *
 * @param {string} resourceClass - "bucket" or "account"
 * @param {string} resourceId - bucket name or account canonicalId
 * @param {string} measure - measure id e.g. "rps"
 * @param {object} limitConfig - Rate limit configuration
 * @param {object} log - Logger instance
 * @returns {WorkerTokenBucket}
 */
function getTokenBucket(resourceClass, resourceId, measure, limitConfig, log) {
    const cacheKey = `${resourceClass}:${resourceId}:${measure}`;
    let bucket = tokenBuckets.get(cacheKey);
    if (!bucket) {
        bucket = new WorkerTokenBucket(resourceClass, resourceId, measure, limitConfig, log);
        tokenBuckets.set(cacheKey, bucket);

        log.debug('Created token bucket', {
            cacheKey,
            bufferSize: bucket.bufferSize,
            refillThreshold: bucket.refillThreshold,
        });
    } else {
        const { updated, oldConfig } = bucket.updateLimit(limitConfig);
        if (updated) {
            log.info('Updated token bucket limit config', {
                cacheKey,
                old: oldConfig,
                new: limitConfig,
            });
        }
    }

    return bucket;
}

/**
 * Get all active token buckets
 *
 * @returns {Map<string, WorkerTokenBucket>}
 */
function getAllTokenBuckets() {
    return tokenBuckets;
}

/**
 * Clean up expired token buckets
 * Called periodically by cleanup job
 *
 * @param {number} maxIdleMs - Remove buckets idle for more than this duration
 * @returns {number} Number of buckets removed
 */
function cleanupTokenBuckets(maxIdleMs = 60000) {
    const now = Date.now();
    const toRemove = [];

    for (const [key, bucket] of tokenBuckets.entries()) {
        const idleTime = now - bucket.lastRefillTime;
        if (idleTime > maxIdleMs && bucket.tokens === 0) {
            toRemove.push(key);
        }
    }

    for (const key of toRemove) {
        tokenBuckets.delete(key);
    }

    return toRemove.length;
}

/**
 * Remove a specific token bucket (used when rate limit config is deleted)
 *
 * @param {string} resourceClass - "bucket" or "account"
 * @param {string} resourceId - bucket name or account canonicalId
 * @param {string} measure - measure id e.g. "rps"
 * @returns {boolean} True if bucket was found and removed
 */
function removeTokenBucket(resourceClass, resourceId, measure) {
    return tokenBuckets.delete(`${resourceClass}:${resourceId}:${measure}`);
}

module.exports = {
    WorkerTokenBucket,
    getTokenBucket,
    getAllTokenBuckets,
    cleanupTokenBuckets,
    removeTokenBucket,
};
