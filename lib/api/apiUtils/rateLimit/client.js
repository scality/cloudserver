const fs = require('fs');

const Redis = require('ioredis');

const { config } = require('../../../Config');

const updateCounterScript = fs.readFileSync(`${__dirname  }/updateCounter.lua`).toString();
const grantTokensScript = fs.readFileSync(`${__dirname  }/grantTokens.lua`).toString();

const SCRIPTS = {
    updateCounter: {
        numberOfKeys: 1,
        lua: updateCounterScript,
    },
    grantTokens: {
        numberOfKeys: 1,
        lua: grantTokensScript,
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
     * @typedef {Object} CounterUpdateBatch
     * @property {string} key - counter key
     * @property {number} cost - per-worker cost to add to counter
     */

    /**
     * @typedef {Object} CounterUpdateBatchResult
     * @property {string} key - counter key
     * @property {number} value - current value of counter after update
     */

    /**
     * @callback RateLimitClient~batchUpdate
     * @param {Error|null} err
     * @param {CounterUpdateBatchResult[]|undefined}
     */

    /**
     * Update local counter values in Redis by adding per-worker costs.
     * Each worker divides its consumption by worker count before syncing.
     * Redis sums all workers' costs to get total node consumption.
     *
     * @param {CounterUpdateBatch[]} batch - batch of counter updates
     * @param {RateLimitClient~batchUpdate} cb
     */
    updateLocalCounters(batch, cb) {
        const pipeline = this.redis.pipeline();
        for (const { key, cost } of batch) {
            pipeline.updateCounter(key, cost);
        }

        pipeline.exec((err, results) => {
            if (err) {
                cb(err);
                return;
            }

            cb(null, results.map((res, i) => ({
                key: batch[i].key,
                value: res[1],
            })));
        });
    }

    /**
     * @callback RateLimitClient~grantTokens
     * @param {Error|null} err
     * @param {number|undefined} granted - Number of tokens granted (0 if denied)
     */

    /**
     * Request tokens from Redis with atomic GCRA enforcement
     *
     * This method atomically:
     * 1. Evaluates GCRA for N tokens
     * 2. Grants tokens if quota available
     * 3. Advances Redis counter by granted tokens
     *
     * Used by token reservation system to request capacity in advance.
     *
     * @param {string} bucketName - Bucket name
     * @param {number} requested - Number of tokens requested
     * @param {number} interval - Interval per request in ms
     * @param {number} burstCapacity - Burst capacity in ms
     * @param {RateLimitClient~grantTokens} cb - Callback
     */
    grantTokens(bucketName, requested, interval, burstCapacity, cb) {
        const key = `throttling:bucket:${bucketName}:rps`;
        const now = Date.now();

        this.redis.grantTokens(
            key,
            requested,
            interval,
            burstCapacity,
            now,
            (err, result) => {
                if (err) {
                    return cb(err);
                }

                // Result is number of tokens granted (0 if denied, partial if limited)
                const granted = parseInt(result, 10);
                return cb(null, granted);
            }
        );
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
