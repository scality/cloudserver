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
     * @typedef {Object} CounterUpdateBatch
     * @property {string} key - counter key
     * @property {number} cost - cost to add to counter
     */

    /**
     * @typedef {Object} CounterUpdateBatchResult
     * @property {string} key - counter key
     * @property {number} value - current value of counter
     */

    /**
     * @callback RateLimitClient~batchUpdate
     * @param {Error|null} err
     * @param {CounterUpdateBatchResult[]|undefined}
     */

    /**
     * Add cost to the counter at key.
     * Returns the new value for  the counter
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
}

let instance;
if (config.rateLimiting.enabled) {
    instance = new RateLimitClient(config.localCache);
}

module.exports = {
    instance,
    RateLimitClient
};
