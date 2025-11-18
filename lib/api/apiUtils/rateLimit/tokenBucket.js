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

const { instance: redisClient } = require('./client');
const { config } = require('../../../Config');
const { calculateInterval } = require('./gcra');

// Map of bucket name -> WorkerTokenBucket instance
const tokenBuckets = new Map();

/**
 * Per-bucket token bucket for a single worker
 */
class WorkerTokenBucket {
    constructor(bucketName, limitConfig, log) {
        this.bucketName = bucketName;
        this.limitConfig = limitConfig;
        this.log = log;

        // Token buffer configuration
        this.tokens = 0;
        this.bufferSize = 50; // Max tokens to hold (5 seconds @ 10 req/s)
        this.refillThreshold = 20; // Trigger refill when below this

        // Refill state
        this.refillInProgress = false;
        this.lastRefillTime = Date.now();
        this.refillCount = 0;

        // Track consumption rate for adaptive refill
        this.requestTimestamps = [];
    }

    /**
     * Try to consume one token (hot path - in-memory only)
     *
     * @returns {boolean} True if request allowed, false if throttled
     */
    tryConsume() {
        // Record request for rate calculation
        this.recordRequest();

        if (this.tokens > 0) {
            this.tokens--;
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

        // Trigger async refill
        await this.refill();
    }

    /**
     * Request tokens from Redis using GCRA enforcement
     *
     * @returns {Promise<void>}
     */
    async refill() {
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
            const burstCapacity = (config.rateLimiting.bucket?.defaultConfig?.requestsPerSecond?.burstCapacity || 1) * 1000;

            // Request tokens from Redis (atomic GCRA enforcement)
            const granted = await new Promise((resolve, reject) => {
                redisClient.grantTokens(
                    this.bucketName,
                    requested,
                    interval,
                    burstCapacity,
                    (err, result) => {
                        if (err) return reject(err);
                        resolve(result);
                    }
                );
            });

            // Add granted tokens to buffer
            this.tokens += granted;
            this.refillCount++;

            const duration = Date.now() - startTime;
            this.lastRefillTime = Date.now();

            this.log.debug('Token refill completed', {
                bucketName: this.bucketName,
                requested,
                granted,
                newBalance: this.tokens,
                durationMs: duration,
                refillCount: this.refillCount,
            });

            // Warn if refill took too long or granted too few
            if (duration > 100) {
                this.log.warn('Slow token refill detected', {
                    bucketName: this.bucketName,
                    durationMs: duration,
                });
            }

            if (granted === 0 && requested > 0) {
                this.log.trace('Token refill denied - quota exhausted', {
                    bucketName: this.bucketName,
                    requested,
                });
            }

        } catch (err) {
            this.log.error('Token refill failed', {
                bucketName: this.bucketName,
                error: err.message,
                stack: err.stack,
            });
        } finally {
            this.refillInProgress = false;
        }
    }

    /**
     * Record request timestamp for rate calculation
     */
    recordRequest() {
        const now = Date.now();
        this.requestTimestamps.push(now);

        // Keep only last 1 second of timestamps
        const cutoff = now - 1000;
        while (this.requestTimestamps.length > 0 && this.requestTimestamps[0] < cutoff) {
            this.requestTimestamps.shift();
        }
    }

    /**
     * Get current request rate (requests per second)
     *
     * @returns {number}
     */
    getCurrentRate() {
        const now = Date.now();
        const cutoff = now - 1000;

        let count = 0;
        for (let i = this.requestTimestamps.length - 1; i >= 0; i--) {
            if (this.requestTimestamps[i] >= cutoff) {
                count++;
            } else {
                break;
            }
        }

        return count;
    }

    /**
     * Get token bucket stats for monitoring
     *
     * @returns {object}
     */
    getStats() {
        return {
            tokens: this.tokens,
            bufferSize: this.bufferSize,
            refillThreshold: this.refillThreshold,
            refillInProgress: this.refillInProgress,
            lastRefillTime: this.lastRefillTime,
            refillCount: this.refillCount,
            currentRate: this.getCurrentRate(),
        };
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
function getTokenBucket(bucketName, limitConfig, log) {
    let bucket = tokenBuckets.get(bucketName);

    if (!bucket) {
        bucket = new WorkerTokenBucket(bucketName, limitConfig, log);
        tokenBuckets.set(bucketName, bucket);

        log.debug('Created token bucket', {
            bucketName,
            bufferSize: bucket.bufferSize,
            refillThreshold: bucket.refillThreshold,
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

module.exports = {
    WorkerTokenBucket,
    getTokenBucket,
    getAllTokenBuckets,
    cleanupTokenBuckets,
};
