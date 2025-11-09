/**
 * Provides Redis-backed GCRA counter synchronization for sharing rate limit
 * state between Node.js workers on the same physical node.
 *
 * ARCHITECTURE:
 *   - 10 Node.js workers per physical CloudServer node
 *   - All workers share 1 local Redis instance (localhost:6379)
 *   - Workers sync TAT (Theoretical Arrival Time) to Redis every 10 seconds
 *   - Workers read from Redis on cache miss
 *
 * BENEFITS:
 *   - Fair quota distribution across workers
 *   - Counter persistence across worker restarts
 *   - Low latency (in-memory GCRA with periodic sync)
 *
 * DATA FORMAT:
 *   Redis stores TAT (Theoretical Arrival Time) as a simple integer timestamp.
 *   This is simpler than the original cost-based approach and avoids data
 *   format mismatches between in-memory and Redis storage.
 */

const fs = require('fs');

const Redis = require('ioredis');

const { config } = require('../../../Config');

const updateCounterScript = fs.readFileSync(`${__dirname  }/updateCounter.lua`).toString();

const SCRIPTS = {
    updateCounter: {
        numberOfKeys: 1,
        lua: updateCounterScript,
    },
};

class RateLimitClient {
    constructor(redisConfig) {
        this.redis = new Redis({
            ...redisConfig,
            scripts: SCRIPTS,
            lazyConnect: true,
        });
    }

    /**
     * Get a single counter's TAT from Redis
     *
     * Used on cache miss to discover shared state from other workers.
     *
     * @param {string} key - Counter key
     * @param {function} cb - Callback(err, tat)
     */
    getCounter(key, cb) {
        this.redis.get(key, (err, value) => {
            if (err) {
                return cb(err);
            }
            // Parse TAT from Redis (returns null if not exists)
            const tat = value ? parseInt(value, 10) : null;
            return cb(null, tat);
        });
    }

    /**
     * Sync multiple counters' TAT values to Redis
     *
     * Used by background worker to persist in-memory counters to Redis.
     * Only updates Redis if the provided TAT is newer than current value.
     *
     * @typedef {Object} CounterSync
     * @property {string} key - Counter key
     * @property {number} tat - Theoretical Arrival Time (ms timestamp)
     *
     * @param {CounterSync[]} batch - Batch of counters to sync
     * @param {function} cb - Callback(err, results)
     */
    syncCounters(batch, cb) {
        if (batch.length === 0) {
            return cb(null, []);
        }

        const pipeline = this.redis.pipeline();
        for (const { key, tat } of batch) {
            // Use updateCounter script which only updates if TAT is newer
            pipeline.updateCounter(key, tat);
        }

        pipeline.exec((err, results) => {
            if (err) {
                return cb(err);
            }

            // Return synced TAT values (may be newer if another worker updated)
            return cb(null, results.map((res, i) => ({
                key: batch[i].key,
                tat: parseInt(res[1], 10),
            })));
        });
    }
}

let instance;
if (config.rateLimiting.enabled) {
    instance = new RateLimitClient(config.localCache);
}

module.exports = {
    instance,
    RateLimitClient
};
