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

// Map of bucket name -> WorkerTokenBucket instance
const tokenBuckets = new Map();

/**
 * Per-bucket token bucket for a single worker
 */
class WorkerTokenBucket {
    constructor(className, resourceId, measure, limitConfig, log) {
        this.className = className;
        this.resourceId = resourceId;
        this.measure = measure;
        this.limitConfig = limitConfig;
        this.log = log;

        this.bufferSize = config.rateLimiting?.tokenBucketBufferSize || 50; // Max tokens to hold
        this.refillThreshold = config.rateLimiting?.tokenBucketRefillThreshold || 20; // Trigger refill when below this
        this.tokens = this.bufferSize; // Start with full buffer for fail-open at startup
    }


    tryConsume() {
        if (this.tokens > 0) {
            this.tokens -= 1;
            return true; // ALLOWED
        }

        return false; // THROTTLED
    }

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
            const nodes = config.rateLimiting.nodes || 1;
            const workers = config.clusters || 1;
            const interval = calculateInterval(this.limitConfig.limit, nodes, workers);
            // const burstCapacitySeconds =
            //     config.rateLimiting.bucket?.defaultConfig?.requestsPerSecond?.burstCapacity || 1;
            // const burstCapacity = this.limitConfig.burstCapacitySeconds * 1000;

            let granted = requested;
            if (redisClient.isReady()) {
                // Request tokens from Redis (atomic GCRA enforcement)
                granted = await util.promisify(redisClient.grantTokens.bind(redisClient))(
                    this.className,
                    this.resourceId,
                    this.measure,
                    requested,
                    interval,
                    this.limitConfig.burstCapacity,
                );
            } else {
                // Connection to redis has failed in some way.
                // Client will be reconnecting in the background.
                // We grant the requested amount of tokens anyway to avoid degrading service availability.
                this.log.warn(
                    'rate limit redis client not connected. granting tokens anyway to avoid service degradation',
                    {
                        className: this.className,
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
                className: this.className,
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
                    className: this.className,
                    resourceId: this.resourceId,
                    measure: this.measure,
                    durationMs: duration,
                });
            }

            if (granted === 0 && requested > 0) {
                this.log.trace('Token refill denied - quota exhausted', {
                    className: this.className,
                    resourceId: this.resourceId,
                    measure: this.measure,
                    requested,
                });
            }

        } catch (err) {
            this.log.error('Token refill failed', {
                className: this.className,
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
 * @param {string} bucketName - Bucket name
 * @param {object} limitConfig - Rate limit configuration
 * @param {object} log - Logger instance
 * @returns {WorkerTokenBucket}
 */
function getTokenBucket(className, resourceId, measure, limitConfig, log) {
    const cacheKey = `${className}:${resourceId}${measure}`;
    let bucket = tokenBuckets.get(cacheKey);

    if (!bucket) {
        bucket = new WorkerTokenBucket(className, resourceId, measure, limitConfig, log);
        tokenBuckets.set(cacheKey, bucket);

        log.debug('Created token bucket', {
            cacheKey,
            bufferSize: bucket.bufferSize,
            refillThreshold: bucket.refillThreshold,
        });
    } else if (bucket.limitConfig.limit !== limitConfig.limit) {
        // Update limit config when it changes dynamically
        const oldLimit = bucket.limitConfig.limit;
        bucket.limitConfig = limitConfig;

        log.info('Updated token bucket limit config', {
            cacheKey,
            oldLimit,
            newLimit: limitConfig.limit,
        });
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

    for (const [bucketName, bucket] of tokenBuckets.entries()) {
        const idleTime = now - bucket.lastRefillTime;
        if (idleTime > maxIdleMs && bucket.tokens === 0) {
            toRemove.push(bucketName);
        }
    }

    for (const bucketName of toRemove) {
        tokenBuckets.delete(bucketName);
    }

    return toRemove.length;
}

/**
 * Remove a specific token bucket (used when rate limit config is deleted)
 *
 * @param {string} bucketName - Bucket name
 * @returns {boolean} True if bucket was found and removed
 */
function removeTokenBucket(bucketName) {
    return tokenBuckets.delete(bucketName);
}

module.exports = {
    WorkerTokenBucket,
    getTokenBucket,
    getAllTokenBuckets,
    cleanupTokenBuckets,
    removeTokenBucket,
};
