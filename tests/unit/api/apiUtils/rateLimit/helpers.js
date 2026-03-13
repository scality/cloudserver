const assert = require('assert');
const sinon = require('sinon');
const { config } = require('../../../../../lib/Config');
const cache = require('../../../../../lib/api/apiUtils/rateLimit/cache');
const helpers = require('../../../../../lib/api/apiUtils/rateLimit/helpers');
const constants = require('../../../../../constants');
const tokenBucket = require('../../../../../lib/api/apiUtils/rateLimit/tokenBucket');

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
        cache.configCache.clear();
        // Clear token buckets
        tokenBucket.getAllTokenBuckets().clear();
    });

    afterEach(() => {
        sandbox.restore();
    });

    describe('extractBucketRateLimitConfig', () => {
        let configStub;

        beforeEach(() => {
            configStub = sandbox.stub(config, 'rateLimiting').value({
                enabled: true,
                bucket: {
                    configCacheTTL: 30000,
                    defaultBurstCapacity: 1,
                },
            });
        });

        it('should extract per-bucket config', () => {
            const bucketName = 'test-bucket';
            const mockBucket = {
                getRateLimitConfiguration: () => ({
                    getData: () => ({
                        RequestsPerSecond: { Limit: 200 },
                    }),
                }),
            };

            const result = helpers.extractBucketRateLimitConfig(mockBucket, bucketName, mockLog);

            assert.deepStrictEqual(result, { limit: 200, burstCapacity: 1000, source: 'bucket' });
        });

        it('should fall back to global default config when no bucket config', () => {
            const bucketName = 'test-bucket';
            const mockBucket = {
                getRateLimitConfiguration: () => null,
            };

            configStub.value({
                enabled: true,
                bucket: {
                    defaultConfig: { limit: 100 },
                    defaultBurstCapacity: 1,
                    configCacheTTL: 30000,
                },
            });

            const result = helpers.extractBucketRateLimitConfig(mockBucket, bucketName, mockLog);

            assert.deepStrictEqual(result, { limit: 100, burstCapacity: 1000, source: 'global' });
        });

        it('should return null when no config exists', () => {
            const bucketName = 'test-bucket';
            const mockBucket = {
                getRateLimitConfiguration: () => null,
            };

            configStub.value({
                enabled: true,
                bucket: {
                    defaultBurstCapacity: 1,
                    configCacheTTL: 30000,
                },
            });

            const result = helpers.extractBucketRateLimitConfig(mockBucket, bucketName, mockLog);

            assert.strictEqual(result, null);
        });

        it('should return null when global default limit is 0', () => {
            const bucketName = 'test-bucket';
            const mockBucket = {
                getRateLimitConfiguration: () => null,
            };

            configStub.value({
                enabled: true,
                bucket: {
                    defaultConfig: { limit: 0 },
                    defaultBurstCapacity: 1,
                    configCacheTTL: 30000,
                },
            });

            const result = helpers.extractBucketRateLimitConfig(mockBucket, bucketName, mockLog);

            assert.strictEqual(result, null);
        });

        it('should use default TTL when configCacheTTL is not set', () => {
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
                bucket: {
                    defaultBurstCapacity: 1,
                },
            });

            sandbox.stub(constants, 'rateLimitDefaultConfigCacheTTL').value(60000);

            const result = helpers.extractBucketRateLimitConfig(mockBucket, bucketName, mockLog);

            assert.deepStrictEqual(result, { limit: 200, burstCapacity: 1000, source: 'bucket' });
        });
    });

    describe('checkRateLimitsForRequest', () => {
        beforeEach(() =>
            sandbox.stub(config, 'rateLimiting').value({
                enabled: true,
                nodes: 1,
                tokenBucketBufferSize: 50,
                tokenBucketRefillThreshold: 20,
                bucket: {
                    defaultBurstCapacity: 1,
                },
            })
        );

        afterEach(() => sinon.restore());

        it('should allow request when checks array is empty', () => {
            const result = helpers.checkRateLimitsForRequest([], mockLog);

            assert.deepStrictEqual(result, { allowed: true });
        });

        it('should allow request when bucket has capacity', () => {
            const check = {
                resourceClass: 'bkt', resourceId: 'test-bucket', measure: 'rps',
                config: { limit: 100, burstCapacity: 1000, source: 'bucket' }, source: 'bucket',
            };

            // Pre-populate token bucket with tokens
            const bucket = tokenBucket.getTokenBucket('bkt', 'test-bucket', 'rps', check.config, mockLog);
            bucket.tokens = 50;

            const result = helpers.checkRateLimitsForRequest([check], mockLog);

            assert.deepStrictEqual(result, { allowed: true });
            // Verify token was consumed
            assert.strictEqual(bucket.tokens, 49);
        });

        it('should deny request when bucket has no tokens', () => {
            const check = {
                resourceClass: 'bkt', resourceId: 'test-bucket', measure: 'rps',
                config: { limit: 100, burstCapacity: 1000, source: 'bucket' }, source: 'bucket',
            };

            const bucket = tokenBucket.getTokenBucket('bkt', 'test-bucket', 'rps', check.config, mockLog);
            // Explicitly set tokens to 0 to simulate exhausted quota
            bucket.tokens = 0;

            const result = helpers.checkRateLimitsForRequest([check], mockLog);

            assert.strictEqual(result.allowed, false);
            assert.strictEqual(result.rateLimitSource, 'bkt:bucket');
        });

        it('should not consume tokens when denied', () => {
            const check = {
                resourceClass: 'bkt', resourceId: 'test-bucket', measure: 'rps',
                config: { limit: 100, burstCapacity: 1000, source: 'bucket' }, source: 'bucket',
            };

            const bucket = tokenBucket.getTokenBucket('bkt', 'test-bucket', 'rps', check.config, mockLog);
            bucket.tokens = 0;

            helpers.checkRateLimitsForRequest([check], mockLog);

            assert.strictEqual(bucket.tokens, 0);
        });

        it('should consume tokens from all buckets when all have capacity', () => {
            const check1 = {
                resourceClass: 'bkt', resourceId: 'bucket-1', measure: 'rps',
                config: { limit: 100, burstCapacity: 1000, source: 'bucket' }, source: 'bucket',
            };
            const check2 = {
                resourceClass: 'acc', resourceId: 'account-1', measure: 'rps',
                config: { limit: 200, burstCapacity: 1000, source: 'account' }, source: 'account',
            };

            const bucket1 = tokenBucket.getTokenBucket('bkt', 'bucket-1', 'rps', check1.config, mockLog);
            const bucket2 = tokenBucket.getTokenBucket('acc', 'account-1', 'rps', check2.config, mockLog);
            bucket1.tokens = 50;
            bucket2.tokens = 50;

            const result = helpers.checkRateLimitsForRequest([check1, check2], mockLog);

            assert.deepStrictEqual(result, { allowed: true });
            assert.strictEqual(bucket1.tokens, 49);
            assert.strictEqual(bucket2.tokens, 49);
        });

        it('should deny on first exhausted bucket and not consume other buckets', () => {
            const check1 = {
                resourceClass: 'bkt', resourceId: 'bucket-1', measure: 'rps',
                config: { limit: 100, burstCapacity: 1000, source: 'bucket' }, source: 'bucket',
            };
            const check2 = {
                resourceClass: 'acc', resourceId: 'account-1', measure: 'rps',
                config: { limit: 200, burstCapacity: 1000, source: 'account' }, source: 'account',
            };

            const bucket1 = tokenBucket.getTokenBucket('bkt', 'bucket-1', 'rps', check1.config, mockLog);
            const bucket2 = tokenBucket.getTokenBucket('acc', 'account-1', 'rps', check2.config, mockLog);
            bucket1.tokens = 0; // exhausted
            bucket2.tokens = 50;

            const result = helpers.checkRateLimitsForRequest([check1, check2], mockLog);

            assert.strictEqual(result.allowed, false);
            // bucket2 tokens should be unchanged (not consumed when an earlier check fails)
            assert.strictEqual(bucket2.tokens, 50);
        });

        it('should log debug info when request is denied', () => {
            const check = {
                resourceClass: 'bkt', resourceId: 'test-bucket', measure: 'rps',
                config: { limit: 100, burstCapacity: 1000, source: 'bucket' }, source: 'bucket',
            };

            const bucket = tokenBucket.getTokenBucket('bkt', 'test-bucket', 'rps', check.config, mockLog);
            // Explicitly set tokens to 0 to trigger denial log
            bucket.tokens = 0;

            helpers.checkRateLimitsForRequest([check], mockLog);

            const deniedCall = mockLog.debug.getCalls().find(
                call => call.args[0] === 'Rate limit check: denied (no tokens available)'
            );
            assert(deniedCall, 'Should have logged denied message');
            const logArgs = deniedCall.args[1];
            assert.strictEqual(logArgs.resourceClass, 'bkt');
            assert.strictEqual(logArgs.resourceId, 'test-bucket');
            assert.strictEqual(logArgs.limit, 100);
            assert.strictEqual(logArgs.source, 'bucket');
        });

        it('should log trace info when request is allowed', () => {
            const check = {
                resourceClass: 'bkt', resourceId: 'test-bucket', measure: 'rps',
                config: { limit: 100, burstCapacity: 1000, source: 'bucket' }, source: 'bucket',
            };

            // Pre-populate token bucket
            const bucket = tokenBucket.getTokenBucket('bkt', 'test-bucket', 'rps', check.config, mockLog);
            bucket.tokens = 50;

            const result = helpers.checkRateLimitsForRequest([check], mockLog);

            assert.strictEqual(result.allowed, true);
            const allowedCall = mockLog.trace.getCalls().find(
                call => call.args[0] === 'Rate limit check: allowed (token consumed)'
            );
            assert(allowedCall, 'Should have logged allowed message');
            assert.strictEqual(allowedCall.args[1].resourceClass, 'bkt');
            assert.strictEqual(allowedCall.args[1].resourceId, 'test-bucket');
        });

        it('should handle multiple sequential requests correctly', () => {
            const check = {
                resourceClass: 'bkt', resourceId: 'test-bucket', measure: 'rps',
                config: { limit: 100, burstCapacity: 1000, source: 'bucket' }, source: 'bucket',
            };

            // Pre-populate token bucket with multiple tokens
            const bucket = tokenBucket.getTokenBucket('bkt', 'test-bucket', 'rps', check.config, mockLog);
            bucket.tokens = 50;

            // First request should be allowed
            const result1 = helpers.checkRateLimitsForRequest([check], mockLog);
            assert.strictEqual(result1.allowed, true);
            assert.strictEqual(bucket.tokens, 49);

            // Second request should also be allowed (still has tokens)
            const result2 = helpers.checkRateLimitsForRequest([check], mockLog);
            assert.strictEqual(result2.allowed, true);
            assert.strictEqual(bucket.tokens, 48);
        });
    });
});
