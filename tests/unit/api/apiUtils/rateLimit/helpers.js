const assert = require('assert');
const sinon = require('sinon');
const { config } = require('../../../../../lib/Config');
const cache = require('../../../../../lib/api/apiUtils/rateLimit/cache');
const helpers = require('../../../../../lib/api/apiUtils/rateLimit/helpers');
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
        cache.bucketOwnerCache.clear();
        // Clear token buckets
        tokenBucket.getAllTokenBuckets().clear();
    });

    afterEach(() => {
        sandbox.restore();
    });

    describe('requestNeedsRateCheck', () => {
        it('should return false when rate limiting is disabled', () => {
            sandbox.stub(config, 'rateLimiting').value({
                enabled: false,
            });

            const request = { apiMethod: 'objectGet' };
            assert.strictEqual(helpers.requestNeedsRateCheck(request), false);
        });

        it('should return false for rate limit admin API actions', () => {
            sandbox.stub(config, 'rateLimiting').value({
                enabled: true,
            });

            for (const action of helpers.rateLimitApiActions) {
                const request = { apiMethod: action };
                assert.strictEqual(
                    helpers.requestNeedsRateCheck(request),
                    false,
                    `Expected false for rate limit action: ${action}`,
                );
            }
        });

        it('should return false for internal service requests', () => {
            sandbox.stub(config, 'rateLimiting').value({
                enabled: true,
            });

            const request = {
                apiMethod: 'objectGet',
                isInternalServiceRequest: true,
            };
            assert.strictEqual(helpers.requestNeedsRateCheck(request), false);
        });

        it('should return false when both bucket and account are already checked', () => {
            sandbox.stub(config, 'rateLimiting').value({
                enabled: true,
            });

            const request = {
                apiMethod: 'objectGet',
                rateLimitAccountAlreadyChecked: true,
                rateLimitBucketAlreadyChecked: true,
            };
            assert.strictEqual(helpers.requestNeedsRateCheck(request), false);
        });

        it('should return true when only bucket is already checked', () => {
            sandbox.stub(config, 'rateLimiting').value({
                enabled: true,
            });

            const request = {
                apiMethod: 'objectGet',
                rateLimitAccountAlreadyChecked: false,
                rateLimitBucketAlreadyChecked: true,
            };
            assert.strictEqual(helpers.requestNeedsRateCheck(request), true);
        });

        it('should return true when only account is already checked', () => {
            sandbox.stub(config, 'rateLimiting').value({
                enabled: true,
            });

            const request = {
                apiMethod: 'objectGet',
                rateLimitAccountAlreadyChecked: true,
                rateLimitBucketAlreadyChecked: false,
            };
            assert.strictEqual(helpers.requestNeedsRateCheck(request), true);
        });

        it('should return true for a normal request needing rate check', () => {
            sandbox.stub(config, 'rateLimiting').value({
                enabled: true,
            });

            const request = {
                apiMethod: 'objectGet',
                isInternalServiceRequest: false,
                rateLimitAccountAlreadyChecked: false,
                rateLimitBucketAlreadyChecked: false,
            };
            assert.strictEqual(helpers.requestNeedsRateCheck(request), true);
        });
    });

    describe('extractBucketRateLimitConfig', () => {
        beforeEach(() => {
            sandbox.stub(config, 'rateLimiting').value({
                enabled: true,
                serviceUserArn: 'foo',
                bucket: {
                    configCacheTTL: 30000,
                    defaultConfig: {
                        RequestsPerSecond: { BurstCapacity: 1 },
                    },
                },
            });
        });

        it('should extract per-bucket config', () => {
            const mockBucket = {
                getName: () => 'test-bucket',
                getRateLimitConfiguration: () => ({
                    getData: () => ({
                        RequestsPerSecond: { Limit: 200 },
                    }),
                }),
            };

            const result = helpers.extractBucketRateLimitConfig(mockBucket, mockLog);

            assert.deepStrictEqual(result, {
                RequestsPerSecond: { BurstCapacity: 1, Limit: 200, source: 'resource' },
            });
        });

        it('should use global defaults when bucket has no per-resource config', () => {
            const mockBucket = {
                getName: () => 'test-bucket',
                getRateLimitConfiguration: () => ({
                    getData: () => ({
                        RequestsPerSecond: undefined,
                    }),
                }),
            };

            const result = helpers.extractBucketRateLimitConfig(mockBucket, mockLog);

            assert.deepStrictEqual(result, {
                RequestsPerSecond: { BurstCapacity: 1, source: 'global' },
            });
        });

        it('should fall back to global default config when no bucket config', () => {
            const mockBucket = {
                getName: () => 'test-bucket',
                getRateLimitConfiguration: () => null,
            };

            sandbox.stub(config, 'rateLimiting').value({
                enabled: true,
                bucket: {
                    defaultConfig: {
                        RequestsPerSecond: { Limit: 100, BurstCapacity: 1 },
                    },
                    configCacheTTL: 30000,
                },
            });

            const result = helpers.extractBucketRateLimitConfig(mockBucket, mockLog);

            assert.deepStrictEqual(result, {
                RequestsPerSecond: { Limit: 100, BurstCapacity: 1, source: 'global' },
            });
        });

        it('should return global defaults with no Limit when defaultConfig has no Limit', () => {
            const mockBucket = {
                getName: () => 'test-bucket',
                getRateLimitConfiguration: () => null,
            };

            const result = helpers.extractBucketRateLimitConfig(mockBucket, mockLog);

            assert.deepStrictEqual(result, {
                RequestsPerSecond: { BurstCapacity: 1, source: 'global' },
            });
        });

        it('should merge per-bucket config over global defaults', () => {
            const mockBucket = {
                getName: () => 'test-bucket',
                getRateLimitConfiguration: () => ({
                    getData: () => ({
                        RequestsPerSecond: { Limit: 500, BurstCapacity: 10 },
                    }),
                }),
            };

            const result = helpers.extractBucketRateLimitConfig(mockBucket, mockLog);

            assert.deepStrictEqual(result, {
                RequestsPerSecond: { BurstCapacity: 10, Limit: 500, source: 'resource' },
            });
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
            }),
        );

        afterEach(() => sinon.restore());

        it('should allow request when checks array is empty', () => {
            const result = helpers.checkRateLimitsForRequest([], mockLog);

            assert.deepStrictEqual(result, { allowed: true });
        });

        it('should allow request when bucket has capacity', () => {
            const check = {
                resourceClass: 'bkt',
                resourceId: 'test-bucket',
                measure: 'rps',
                config: { limit: 100, burstCapacity: 1000, source: 'bucket' },
                source: 'bucket',
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
                resourceClass: 'bkt',
                resourceId: 'test-bucket',
                measure: 'rps',
                config: { limit: 100, burstCapacity: 1000, source: 'bucket' },
                source: 'bucket',
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
                resourceClass: 'bkt',
                resourceId: 'test-bucket',
                measure: 'rps',
                config: { limit: 100, burstCapacity: 1000, source: 'bucket' },
                source: 'bucket',
            };

            const bucket = tokenBucket.getTokenBucket('bkt', 'test-bucket', 'rps', check.config, mockLog);
            bucket.tokens = 0;

            helpers.checkRateLimitsForRequest([check], mockLog);

            assert.strictEqual(bucket.tokens, 0);
        });

        it('should consume tokens from all buckets when all have capacity', () => {
            const check1 = {
                resourceClass: 'bkt',
                resourceId: 'bucket-1',
                measure: 'rps',
                config: { limit: 100, burstCapacity: 1000, source: 'bucket' },
                source: 'bucket',
            };
            const check2 = {
                resourceClass: 'acc',
                resourceId: 'account-1',
                measure: 'rps',
                config: { limit: 200, burstCapacity: 1000, source: 'account' },
                source: 'account',
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
                resourceClass: 'bkt',
                resourceId: 'bucket-1',
                measure: 'rps',
                config: { limit: 100, burstCapacity: 1000, source: 'bucket' },
                source: 'bucket',
            };
            const check2 = {
                resourceClass: 'acc',
                resourceId: 'account-1',
                measure: 'rps',
                config: { limit: 200, burstCapacity: 1000, source: 'account' },
                source: 'account',
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
                resourceClass: 'bkt',
                resourceId: 'test-bucket',
                measure: 'rps',
                config: { limit: 100, burstCapacity: 1000, source: 'bucket' },
                source: 'bucket',
            };

            const bucket = tokenBucket.getTokenBucket('bkt', 'test-bucket', 'rps', check.config, mockLog);
            // Explicitly set tokens to 0 to trigger denial log
            bucket.tokens = 0;

            helpers.checkRateLimitsForRequest([check], mockLog);

            const deniedCall = mockLog.debug
                .getCalls()
                .find(call => call.args[0] === 'Rate limit check: denied (no tokens available)');
            assert(deniedCall, 'Should have logged denied message');
            const logArgs = deniedCall.args[1];
            assert.strictEqual(logArgs.resourceClass, 'bkt');
            assert.strictEqual(logArgs.resourceId, 'test-bucket');
            assert.strictEqual(logArgs.limit, 100);
            assert.strictEqual(logArgs.source, 'bucket');
        });

        it('should log trace info when request is allowed', () => {
            const check = {
                resourceClass: 'bkt',
                resourceId: 'test-bucket',
                measure: 'rps',
                config: { limit: 100, burstCapacity: 1000, source: 'bucket' },
                source: 'bucket',
            };

            // Pre-populate token bucket
            const bucket = tokenBucket.getTokenBucket('bkt', 'test-bucket', 'rps', check.config, mockLog);
            bucket.tokens = 50;

            const result = helpers.checkRateLimitsForRequest([check], mockLog);

            assert.strictEqual(result.allowed, true);
            const allowedCall = mockLog.trace
                .getCalls()
                .find(call => call.args[0] === 'Rate limit check: allowed (token consumed)');
            assert(allowedCall, 'Should have logged allowed message');
            assert.strictEqual(allowedCall.args[1].resourceClass, 'bkt');
            assert.strictEqual(allowedCall.args[1].resourceId, 'test-bucket');
        });

        it('should handle multiple sequential requests correctly', () => {
            const check = {
                resourceClass: 'bkt',
                resourceId: 'test-bucket',
                measure: 'rps',
                config: { limit: 100, burstCapacity: 1000, source: 'bucket' },
                source: 'bucket',
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

    describe('extractRateLimitConfigFromRequest', () => {
        beforeEach(() => {
            sandbox.stub(config, 'rateLimiting').value({
                enabled: true,
                serviceUserArn: 'foo',
                bucket: {
                    configCacheTTL: 30000,
                    defaultConfig: {
                        RequestsPerSecond: { BurstCapacity: 1 },
                    },
                },
                account: {
                    configCacheTTL: 30000,
                    defaultConfig: {
                        RequestsPerSecond: { BurstCapacity: 2 },
                    },
                },
            });
        });

        it('should return both bucket and account configs', () => {
            const mockBucket = {
                getName: () => 'test-bucket',
                getRateLimitConfiguration: () => ({
                    getData: () => ({
                        RequestsPerSecond: { Limit: 200 },
                    }),
                }),
            };
            const mockAuthInfo = {
                getCanonicalID: () => 'account-123',
            };
            const request = {
                accountLimits: {
                    RequestsPerSecond: { Limit: 500 },
                },
            };

            const result = helpers.extractRateLimitConfigFromRequest(request, mockAuthInfo, mockBucket, mockLog);

            assert.deepStrictEqual(result.bucket, {
                RequestsPerSecond: { BurstCapacity: 1, Limit: 200, source: 'resource' },
            });
            assert.deepStrictEqual(result.account, {
                RequestsPerSecond: { BurstCapacity: 2, Limit: 500, source: 'resource' },
            });
        });

        it('should use global defaults when no per-resource configs exist', () => {
            const mockBucket = {
                getName: () => 'test-bucket',
                getRateLimitConfiguration: () => null,
            };
            const mockAuthInfo = {
                getCanonicalID: () => 'account-123',
            };
            const request = {};

            const result = helpers.extractRateLimitConfigFromRequest(request, mockAuthInfo, mockBucket, mockLog);

            assert.deepStrictEqual(result.bucket, {
                RequestsPerSecond: { BurstCapacity: 1, source: 'global' },
            });
            assert.deepStrictEqual(result.account, {
                RequestsPerSecond: { BurstCapacity: 2, source: 'global' },
            });
        });

        it('should extract per-account config with resource source', () => {
            const mockBucket = {
                getName: () => 'test-bucket',
                getRateLimitConfiguration: () => null,
            };
            const mockAuthInfo = {
                getCanonicalID: () => 'account-123',
            };
            const request = {
                accountLimits: {
                    RequestsPerSecond: { Limit: 300, BurstCapacity: 5 },
                },
            };

            const result = helpers.extractRateLimitConfigFromRequest(request, mockAuthInfo, mockBucket, mockLog);

            assert.deepStrictEqual(result.account, {
                RequestsPerSecond: { BurstCapacity: 5, Limit: 300, source: 'resource' },
            });
        });

        it('should use global source when accountLimits has no RequestsPerSecond', () => {
            const mockBucket = {
                getName: () => 'test-bucket',
                getRateLimitConfiguration: () => null,
            };
            const mockAuthInfo = {
                getCanonicalID: () => 'account-123',
            };
            const request = {
                accountLimits: {
                    RequestsPerSecond: undefined,
                },
            };

            const result = helpers.extractRateLimitConfigFromRequest(request, mockAuthInfo, mockBucket, mockLog);

            assert.deepStrictEqual(result.account, {
                RequestsPerSecond: { BurstCapacity: 2, source: 'global' },
            });
        });
    });

    describe('buildRateChecksFromConfig', () => {
        it('should build a check when Limit is set and positive', () => {
            const limitConfig = {
                RequestsPerSecond: { Limit: 100, BurstCapacity: 2, source: 'resource' },
            };

            const checks = helpers.buildRateChecksFromConfig('bkt', 'test-bucket', limitConfig);

            assert.strictEqual(checks.length, 1);
            assert.deepStrictEqual(checks[0], {
                resourceClass: 'bkt',
                resourceId: 'test-bucket',
                measure: 'rps',
                source: 'resource',
                config: {
                    limit: 100,
                    burstCapacity: 2000,
                },
            });
        });

        it('should return empty array when Limit is 0', () => {
            const limitConfig = {
                RequestsPerSecond: { Limit: 0, BurstCapacity: 2, source: 'global' },
            };

            const checks = helpers.buildRateChecksFromConfig('bkt', 'test-bucket', limitConfig);

            assert.strictEqual(checks.length, 0);
        });

        it('should return empty array when Limit is undefined', () => {
            const limitConfig = {
                RequestsPerSecond: { BurstCapacity: 2, source: 'global' },
            };

            const checks = helpers.buildRateChecksFromConfig('bkt', 'test-bucket', limitConfig);

            assert.strictEqual(checks.length, 0);
        });

        it('should return empty array when limitConfig is null', () => {
            const checks = helpers.buildRateChecksFromConfig('bkt', 'test-bucket', null);

            assert.strictEqual(checks.length, 0);
        });

        it('should return empty array when limitConfig is undefined', () => {
            const checks = helpers.buildRateChecksFromConfig('bkt', 'test-bucket', undefined);

            assert.strictEqual(checks.length, 0);
        });

        it('should return empty array when RequestsPerSecond is missing', () => {
            const checks = helpers.buildRateChecksFromConfig('bkt', 'test-bucket', {});

            assert.strictEqual(checks.length, 0);
        });

        it('should multiply BurstCapacity by 1000', () => {
            const limitConfig = {
                RequestsPerSecond: { Limit: 50, BurstCapacity: 5, source: 'global' },
            };

            const checks = helpers.buildRateChecksFromConfig('acc', 'account-1', limitConfig);

            assert.strictEqual(checks[0].config.burstCapacity, 5000);
        });

        it('should return empty array when Limit is negative', () => {
            const limitConfig = {
                RequestsPerSecond: { Limit: -1, BurstCapacity: 2, source: 'global' },
            };

            const checks = helpers.buildRateChecksFromConfig('bkt', 'test-bucket', limitConfig);

            assert.strictEqual(checks.length, 0);
        });
    });

    describe('getCachedRateLimitConfig', () => {
        it('should return empty object when nothing is cached', () => {
            const request = { bucketName: 'test-bucket' };

            const result = helpers.getCachedRateLimitConfig(request);

            assert.deepStrictEqual(result, {});
        });

        it('should return cached bucket config when available', () => {
            const bucketConfig = {
                RequestsPerSecond: { Limit: 100, source: 'resource' },
            };
            cache.setCachedConfig(cache.namespace.bucket, 'test-bucket', bucketConfig, 30000);

            const request = { bucketName: 'test-bucket' };
            const result = helpers.getCachedRateLimitConfig(request);

            assert.deepStrictEqual(result.bucket, bucketConfig);
            assert.strictEqual(result.account, undefined);
        });

        it('should return cached account config when bucket owner and account config are cached', () => {
            const accountConfig = {
                RequestsPerSecond: { Limit: 500, source: 'global' },
            };
            cache.setCachedBucketOwner('test-bucket', 'owner-123', 30000);
            cache.setCachedConfig(cache.namespace.account, 'owner-123', accountConfig, 30000);

            const request = { bucketName: 'test-bucket' };
            const result = helpers.getCachedRateLimitConfig(request);

            assert.deepStrictEqual(result.account, accountConfig);
            assert.strictEqual(result.bucketOwner, 'owner-123');
        });

        it('should return both bucket and account configs when all are cached', () => {
            const bucketConfig = {
                RequestsPerSecond: { Limit: 100, source: 'resource' },
            };
            const accountConfig = {
                RequestsPerSecond: { Limit: 500, source: 'global' },
            };
            cache.setCachedConfig(cache.namespace.bucket, 'test-bucket', bucketConfig, 30000);
            cache.setCachedBucketOwner('test-bucket', 'owner-123', 30000);
            cache.setCachedConfig(cache.namespace.account, 'owner-123', accountConfig, 30000);

            const request = { bucketName: 'test-bucket' };
            const result = helpers.getCachedRateLimitConfig(request);

            assert.deepStrictEqual(result.bucket, bucketConfig);
            assert.deepStrictEqual(result.account, accountConfig);
            assert.strictEqual(result.bucketOwner, 'owner-123');
        });

        it('should not return account config when bucket owner is cached but account config is not', () => {
            cache.setCachedBucketOwner('test-bucket', 'owner-123', 30000);

            const request = { bucketName: 'test-bucket' };
            const result = helpers.getCachedRateLimitConfig(request);

            assert.strictEqual(result.account, undefined);
            assert.strictEqual(result.bucketOwner, undefined);
        });

        it('should not return account config when bucket owner is not cached', () => {
            const accountConfig = {
                RequestsPerSecond: { Limit: 500, source: 'global' },
            };
            cache.setCachedConfig(cache.namespace.account, 'owner-123', accountConfig, 30000);

            const request = { bucketName: 'test-bucket' };
            const result = helpers.getCachedRateLimitConfig(request);

            assert.strictEqual(result.account, undefined);
        });
    });
});
