const assert = require('assert');
const sinon = require('sinon');
const { config } = require('../../../../../lib/Config');
const cache = require('../../../../../lib/api/apiUtils/rateLimit/cache');
const helpers = require('../../../../../lib/api/apiUtils/rateLimit/helpers');
const constants = require('../../../../../constants');

describe('Rate limit helpers', () => {
    let sandbox;
    let mockLog;

    beforeEach(() => {
        sandbox = sinon.createSandbox();
        mockLog = {
            trace: sinon.stub(),
            debug: sinon.stub(),
            info: sinon.stub(),
            error: sinon.stub(),
        };
        // Clear cache before each test
        cache.counters.clear();
        cache.configCache.clear();
    });

    afterEach(() => {
        sandbox.restore();
    });

    describe('getRateLimitFromCache', () => {
        it('should return cached config on cache hit', () => {
            const bucketName = 'test-bucket';
            const limitConfig = { limit: 100, source: 'bucket' };
            cache.setCachedConfig(`bucket:${bucketName}`, limitConfig, 60000);

            const result = helpers.getRateLimitFromCache(bucketName);

            assert.strictEqual(result, limitConfig);
        });

        it('should return undefined on cache miss', () => {
            const bucketName = 'test-bucket';

            const result = helpers.getRateLimitFromCache(bucketName);

            assert.strictEqual(result, undefined);
        });

        it('should return null when null is cached', () => {
            const bucketName = 'test-bucket';
            cache.setCachedConfig(`bucket:${bucketName}`, null, 60000);

            const result = helpers.getRateLimitFromCache(bucketName);

            assert.strictEqual(result, null);
        });
    });

    describe('extractAndCacheRateLimitConfig', () => {
        let configStub;

        beforeEach(() => {
            configStub = sandbox.stub(config, 'rateLimiting').value({
                enabled: true,
                bucket: {
                    configCacheTTL: 30000,
                },
            });
        });

        it('should extract and cache per-bucket config', () => {
            const bucketName = 'test-bucket';
            const mockBucket = {
                getRateLimitConfiguration: () => ({
                    getData: () => ({
                        RequestsPerSecond: { Limit: 200 },
                    }),
                }),
            };

            const result = helpers.extractAndCacheRateLimitConfig(mockBucket, bucketName, mockLog);

            assert.deepStrictEqual(result, { limit: 200, source: 'bucket' });
            // Verify it was cached
            const cached = cache.getCachedConfig(`bucket:${bucketName}`);
            assert.deepStrictEqual(cached, { limit: 200, source: 'bucket' });
        });

        it('should fall back to global config when no bucket config', () => {
            const bucketName = 'test-bucket';
            const mockBucket = {
                getRateLimitConfiguration: () => null,
            };

            configStub.value({
                enabled: true,
                bucket: {
                    defaultConfig: { limit: 100 },
                    configCacheTTL: 30000,
                },
            });

            const result = helpers.extractAndCacheRateLimitConfig(mockBucket, bucketName, mockLog);

            assert.deepStrictEqual(result, { limit: 100, source: 'global' });
            // Verify it was cached
            const cached = cache.getCachedConfig(`bucket:${bucketName}`);
            assert.deepStrictEqual(cached, { limit: 100, source: 'global' });
        });

        it('should return null when no config exists', () => {
            const bucketName = 'test-bucket';
            const mockBucket = {
                getRateLimitConfiguration: () => null,
            };

            configStub.value({
                enabled: true,
                bucket: {
                    configCacheTTL: 30000,
                },
            });

            const result = helpers.extractAndCacheRateLimitConfig(mockBucket, bucketName, mockLog);

            assert.strictEqual(result, null);
            // Verify null was cached
            const cached = cache.getCachedConfig(`bucket:${bucketName}`);
            assert.strictEqual(cached, null);
        });

        it('should skip global default if limit is 0', () => {
            const bucketName = 'test-bucket';
            const mockBucket = {
                getRateLimitConfiguration: () => null,
            };

            configStub.value({
                enabled: true,
                bucket: {
                    defaultConfig: { limit: 0 },
                    configCacheTTL: 30000,
                },
            });

            const result = helpers.extractAndCacheRateLimitConfig(mockBucket, bucketName, mockLog);

            assert.strictEqual(result, null);
        });

        it('should use default TTL when not configured', () => {
            const bucketName = 'test-bucket';
            const mockBucket = {
                getRateLimitConfiguration: () => ({
                    getData: () => ({
                        RequestsPerSecond: { Limit: 200 },
                    }),
                }),
            };

            configStub.value({
                enabled: true,
                bucket: {},
            });

            sandbox.stub(constants, 'rateLimitDefaultConfigCacheTTL').value(60000);

            helpers.extractAndCacheRateLimitConfig(mockBucket, bucketName, mockLog);

            // Verify it was cached with default TTL
            const cached = cache.getCachedConfig(`bucket:${bucketName}`);
            assert.deepStrictEqual(cached, { limit: 200, source: 'bucket' });
        });
    });

    describe('checkRateLimitWithConfig', () => {
        let configStub;

        beforeEach(() => {
            configStub = sandbox.stub(config, 'rateLimiting').value({
                enabled: true,
                nodes: 1,
                bucket: {
                    defaultBurstCapacity: 1,
                },
            });
            sandbox.stub(config, 'clusters').value(1);
        });

        it('should allow request when no limit configured', done => {
            const bucketName = 'test-bucket';
            const limitConfig = null;

            helpers.checkRateLimitWithConfig(bucketName, limitConfig, mockLog, (err, rateLimited) => {
                assert.strictEqual(err, null);
                assert.strictEqual(rateLimited, false);
                done();
            });
        });

        it('should allow request when limit is 0', done => {
            const bucketName = 'test-bucket';
            const limitConfig = { limit: 0, source: 'global' };

            helpers.checkRateLimitWithConfig(bucketName, limitConfig, mockLog, (err, rateLimited) => {
                assert.strictEqual(err, null);
                assert.strictEqual(rateLimited, false);
                done();
            });
        });

        it('should allow request when bucket has capacity', done => {
            const bucketName = 'test-bucket';
            const limitConfig = { limit: 100, source: 'bucket' };

            helpers.checkRateLimitWithConfig(bucketName, limitConfig, mockLog, (err, rateLimited) => {
                assert.strictEqual(err, null);
                assert.strictEqual(rateLimited, false);
                // Verify counter was updated
                const counter = cache.getCounter(`bucket:${bucketName}:rps`);
                assert(counter > 0);
                done();
            });
        });

        it('should deny request when bucket is full', done => {
            const bucketName = 'test-bucket';
            const limitConfig = { limit: 100, source: 'bucket' };

            // Pre-fill the bucket by setting counter to future
            const now = Date.now();
            cache.setCounter(`bucket:${bucketName}:rps`, now + 10000);

            helpers.checkRateLimitWithConfig(bucketName, limitConfig, mockLog, (err, rateLimited) => {
                assert.strictEqual(err, null);
                assert.strictEqual(rateLimited, true);
                done();
            });
        });

        it('should use configured burst capacity', done => {
            const bucketName = 'test-bucket';
            const limitConfig = { limit: 100, source: 'bucket' };

            configStub.value({
                enabled: true,
                nodes: 1,
                bucket: {
                    defaultBurstCapacity: 2,
                },
            });

            helpers.checkRateLimitWithConfig(bucketName, limitConfig, mockLog, (err, rateLimited) => {
                assert.strictEqual(err, null);
                assert.strictEqual(rateLimited, false);
                done();
            });
        });

        it('should use default burst capacity when not configured', done => {
            const bucketName = 'test-bucket';
            const limitConfig = { limit: 100, source: 'bucket' };

            configStub.value({
                enabled: true,
                nodes: 1,
                bucket: {},
            });

            sandbox.stub(constants, 'rateLimitDefaultBurstCapacity').value(1);

            helpers.checkRateLimitWithConfig(bucketName, limitConfig, mockLog, (err, rateLimited) => {
                assert.strictEqual(err, null);
                assert.strictEqual(rateLimited, false);
                done();
            });
        });

        it('should calculate interval for distributed setup', done => {
            const bucketName = 'test-bucket';
            const limitConfig = { limit: 1000, source: 'global' };

            configStub.value({
                enabled: true,
                nodes: 10,
                bucket: {
                    defaultBurstCapacity: 1,
                },
            });
            sandbox.stub(config, 'clusters').value(5);

            helpers.checkRateLimitWithConfig(bucketName, limitConfig, mockLog, (err, rateLimited) => {
                assert.strictEqual(err, null);
                // Should work correctly with distributed calculation
                done();
            });
        });

        it('should log debug info before GCRA evaluation', done => {
            const bucketName = 'test-bucket';
            const limitConfig = { limit: 100, source: 'bucket' };

            helpers.checkRateLimitWithConfig(bucketName, limitConfig, mockLog, () => {
                assert(mockLog.debug.calledWith('Checking rate limit with GCRA'));
                const logArgs = mockLog.debug.firstCall.args[1];
                assert.strictEqual(logArgs.bucketName, bucketName);
                assert.strictEqual(logArgs.limit, 100);
                assert.strictEqual(logArgs.source, 'bucket');
                done();
            });
        });

        it('should log debug info when request allowed', done => {
            const bucketName = 'test-bucket';
            const limitConfig = { limit: 100, source: 'bucket' };

            helpers.checkRateLimitWithConfig(bucketName, limitConfig, mockLog, (err, rateLimited) => {
                assert.strictEqual(rateLimited, false);
                // Find the "allowed" log call
                const allowedCall = mockLog.debug.getCalls().find(
                    call => call.args[0] === 'Rate limit check: allowed'
                );
                assert(allowedCall, 'Should have logged allowed message');
                assert.strictEqual(allowedCall.args[1].bucketName, bucketName);
                assert(allowedCall.args[1].newEmptyAt > 0);
                done();
            });
        });

        it('should log debug info when request denied with retry info', done => {
            const bucketName = 'test-bucket';
            const limitConfig = { limit: 100, source: 'bucket' };

            // Pre-fill the bucket
            const now = Date.now();
            cache.setCounter(`bucket:${bucketName}:rps`, now + 10000);

            helpers.checkRateLimitWithConfig(bucketName, limitConfig, mockLog, (err, rateLimited) => {
                assert.strictEqual(rateLimited, true);
                // Find the "denied" log call
                const deniedCall = mockLog.debug.getCalls().find(
                    call => call.args[0] === 'Rate limit check: denied'
                );
                assert(deniedCall, 'Should have logged denied message');
                assert.strictEqual(deniedCall.args[1].bucketName, bucketName);
                assert.strictEqual(deniedCall.args[1].limit, 100);
                assert(deniedCall.args[1].allowAt > now);
                assert(deniedCall.args[1].retryAfterMs > 0);
                done();
            });
        });

        it('should handle multiple sequential requests correctly', done => {
            const bucketName = 'test-bucket';
            const limitConfig = { limit: 100, source: 'bucket' };

            // First request should be allowed
            helpers.checkRateLimitWithConfig(bucketName, limitConfig, mockLog, (err, rateLimited) => {
                assert.strictEqual(err, null);
                assert.strictEqual(rateLimited, false);

                // Second request should also be allowed (has capacity)
                helpers.checkRateLimitWithConfig(bucketName, limitConfig, mockLog, (err2, rateLimited2) => {
                    assert.strictEqual(err2, null);
                    assert.strictEqual(rateLimited2, false);
                    done();
                });
            });
        });
    });
});
