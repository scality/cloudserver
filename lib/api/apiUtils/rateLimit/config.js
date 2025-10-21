const assert = require('assert');

const { rateLimitDefaultBurstCapacity } = require('../../../../constants');

function parseRateLimitConfig(config) {
    const limitConfig = {};

    if (config.requestsPerSecond) {
        assert.strictEqual(typeof config.requestsPerSecond, 'object');

        const { limit } = config.requestsPerSecond;
        assert(typeof limit === 'number' && Number.isInteger(limit) && limit > 0);

        let { burstCapacity } = config.requestsPerSecond;
        if (burstCapacity !== undefined) {
            assert(typeof burstCapacity === 'number' && Number.isInteger(burstCapacity) && burstCapacity > 0);
        } else {
            burstCapacity = rateLimitDefaultBurstCapacity;
        }

        limitConfig.requestsPerSecond = {
            interval: Math.ceil((1 / limit) * 1000),
            bucketSize: burstCapacity * 1000,
        };
    }
}

module.exports = {
    parseRateLimitConfig,
};
