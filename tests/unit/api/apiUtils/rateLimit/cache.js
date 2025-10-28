const assert = require('assert');
const sinon = require('sinon');

const constants = require('../../../../../constants');
const {
    counters,
    configCache,
    getCounter,
    setCounter,
    expireCounters,
    getCachedConfig,
    setCachedConfig,
    expireCachedConfigs,
} = require('../../../../../lib/api/apiUtils/rateLimit/cache');

describe('test counter storage', () => {
    it('setCounter() should set a counter', () => {
        setCounter('foo', 10);
        assert.strictEqual(counters.get('foo'), 10);
    });

    it('getCounter() should get a counter', () => {
        setCounter('foo', 10);
        assert.strictEqual(getCounter('foo'), 10);
    });

    it('should maintain order when updating a counter', () => {
        setCounter('foo', 10);
        setCounter('bar', 20);
        setCounter('foo', 30);

        const items = Array.from(counters.entries());
        assert.deepStrictEqual(items, [
            ['bar', 20],
            ['foo', 30],
        ]);
    });

    it('should expire counters less than or equal to the given timestamp', () => {
        const now = Date.now();
        const past = now - 100;
        const future = now + 100;
        setCounter('past', past);
        setCounter('present', now);
        setCounter('future', future);
        expireCounters(now);
        assert.strictEqual(getCounter('past'), undefined);
        assert.strictEqual(getCounter('present'), undefined);
        assert.strictEqual(getCounter('future'), future);
    });
});

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
});
