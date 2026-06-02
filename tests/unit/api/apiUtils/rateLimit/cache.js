const assert = require('assert');
const sinon = require('sinon');

const constants = require('../../../../../constants');
const {
    namespace,
    configCache,
    getCachedConfig,
    setCachedConfig,
    expireCachedConfigs,
    deleteCachedConfig,
    MAX_CACHE_ITEMS,
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

    beforeEach(() => {
        configCache.clear();
    });

    it('should add config to cache', () => {
        setCachedConfig(namespace.bucket, 'foo', 10, constants.rateLimitDefaultConfigCacheTTL);
        assert.deepStrictEqual(
            configCache.get(`${namespace.bucket}:foo`),
            {
                expiry: now + constants.rateLimitDefaultConfigCacheTTL,
                value: 10,
            }
        );
    });

    it('should get a non expired config', () => {
        setCachedConfig(namespace.bucket, 'foo', 10, constants.rateLimitDefaultConfigCacheTTL);
        assert.strictEqual(getCachedConfig(namespace.bucket, 'foo'), 10);
    });

    it('should return undefined and delete the key for an expired config', () => {
        configCache.set(`${namespace.bucket}:foo`, {
            expiry: now - 10000,
            value: 10,
        });
        assert.strictEqual(getCachedConfig(namespace.bucket, 'foo'), undefined);
    });

    it('should expire configs less than or equal to current time', () => {
        configCache.set('past', {
            expiry: now - 10000,
            value: 10,
        });
        configCache.set('present', {
            expiry: now,
            value: 10,
        });
        configCache.set('future', {
            expiry: now + 10000,
            value: 10,
        });
        // expireCachedConfigs uses Date.now() internally; fake clock is set to `now`
        expireCachedConfigs();
        assert.strictEqual(configCache.get('past'), undefined);
        assert.strictEqual(configCache.get('present'), undefined);
        assert.deepStrictEqual(configCache.get('future'), {
            expiry: now + 10000,
            value: 10,
        });
    });

    it('should trim cache to MAX_CACHE_ITEMS length', () => {
        for (let i = 0; i < MAX_CACHE_ITEMS + 10; i++) {
            configCache.set(`item-${i}`, {
                expiry: now + 10000,
                value: i
            });
        }

        // expireCachedConfigs uses Date.now() internally; fake clock is set to `now`
        const numDeleted = expireCachedConfigs();
        assert.strictEqual(numDeleted, 10);
        assert.strictEqual(configCache.size, MAX_CACHE_ITEMS);
        assert.strictEqual(configCache.get('item-0'), undefined);
        assert.deepStrictEqual(configCache.get('item-10'), {
            expiry: now + 10000,
            value: 10,
        });
    });

    it('should delete cached config for a specific resource', () => {
        setCachedConfig(namespace.bucket, 'my-bucket', { limit: 100 }, constants.rateLimitDefaultConfigCacheTTL);
        setCachedConfig(namespace.bucket, 'other-bucket', { limit: 200 }, constants.rateLimitDefaultConfigCacheTTL);

        deleteCachedConfig(namespace.bucket, 'my-bucket');

        assert.strictEqual(getCachedConfig(namespace.bucket, 'my-bucket'), undefined);
        assert.deepStrictEqual(getCachedConfig(namespace.bucket, 'other-bucket'), { limit: 200 });
    });

    it('should be a no-op when deleting a non-existent key', () => {
        assert.doesNotThrow(() => {
            deleteCachedConfig(namespace.bucket, 'non-existent-bucket');
        });
    });
});
