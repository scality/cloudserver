const assert = require('assert');
const sinon = require('sinon');

const constants = require('../../../../../constants');
const {
    configCache,
    getCachedConfig,
    setCachedConfig,
    expireCachedConfigs,
    deleteCachedConfig,
} = require('../../../../../lib/api/apiUtils/rateLimit/cache');

describe('test limit config cache storage', () => {
    const now = Date.now();

    let clock;
    before(() => {
        clock = sinon.useFakeTimers(now);
    });

    after(() => {
        clock.restore();
    });

    it('should add config to cache', () => {
        setCachedConfig('foo', 10, constants.rateLimitDefaultConfigCacheTTL);
        assert.deepStrictEqual(
            configCache.get('foo'),
            {
                expiry: now + constants.rateLimitDefaultConfigCacheTTL,
                config: 10,
            }
        );
    });

    it('should get a non expired config', () => {
        setCachedConfig('foo', 10, constants.rateLimitDefaultConfigCacheTTL);
        assert.strictEqual(getCachedConfig('foo'), 10);
    });

    it('should return undefined and delete the key for an expired config', () => {
        configCache.set('foo', {
            expiry: now - 10000,
            config: 10,
        });
        assert.strictEqual(getCachedConfig('foo'), undefined);
    });

    it('should expire configs less than or equal to the given timestamp', () => {
        configCache.set('past', {
            expiry: now - 10000,
            config: 10,
        });
        configCache.set('present', {
            expiry: now,
            config: 10,
        });
        configCache.set('future', {
            expiry: now + 10000,
            config: 10,
        });
        expireCachedConfigs(now);
        assert.strictEqual(configCache.get('past'), undefined);
        assert.strictEqual(configCache.get('present'), undefined);
        assert.deepStrictEqual(configCache.get('future'), {
            expiry: now + 10000,
            config: 10,
        });
    });

    it('should invalidate cached config for a specific bucket', () => {
        setCachedConfig('bucket:my-bucket', { limit: 100 }, constants.rateLimitDefaultConfigCacheTTL);
        setCachedConfig('bucket:other-bucket', { limit: 200 }, constants.rateLimitDefaultConfigCacheTTL);

        const result = deleteCachedConfig('my-bucket');

        assert.strictEqual(result, true);
        assert.strictEqual(getCachedConfig('bucket:my-bucket'), undefined);
        assert.deepStrictEqual(getCachedConfig('bucket:other-bucket'), { limit: 200 });
    });

    it('should return false when invalidating non-existent bucket', () => {
        const result = deleteCachedConfig('non-existent-bucket');

        assert.strictEqual(result, false);
    });
});
