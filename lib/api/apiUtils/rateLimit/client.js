const fs = require('fs');

const Redis = require('ioredis');

const { config } = require('../../../Config');

const grantTokensScript = fs.readFileSync(`${__dirname  }/grantTokens.lua`).toString();

const SCRIPTS = {
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
        const key = `ratelimit:bucket:${bucketName}:rps`;
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
