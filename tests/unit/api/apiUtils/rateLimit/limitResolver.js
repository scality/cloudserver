const assert = require('assert');
const sinon = require('sinon');

const { resolveRateLimit } = require('../../../../../lib/api/apiUtils/rateLimit/limitResolver');
const cache = require('../../../../../lib/api/apiUtils/rateLimit/cache');
const metadata = require('../../../../../lib/metadata/wrapper');
const { config } = require('../../../../../lib/Config');
const constants = require('../../../../../constants');

describe('Rate limit config resolver', () => {
    let mockLog;
    let getCachedConfigStub;
    let setCachedConfigStub;
    let getBucketStub;

    beforeEach(() => {
        mockLog = {
            trace: sinon.stub(),
            debug: sinon.stub(),
        };
        getCachedConfigStub = sinon.stub(cache, 'getCachedConfig');
        setCachedConfigStub = sinon.stub(cache, 'setCachedConfig');
        getBucketStub = sinon.stub(metadata, 'getBucket');
    });

    afterEach(() => {
        getCachedConfigStub.restore();
        setCachedConfigStub.restore();
        getBucketStub.restore();
    });

    describe('cache behavior', () => {
        it('should return cached config on cache hit', done => {
            const cachedConfig = { limit: 100, source: 'bucket' };
            getCachedConfigStub.returns(cachedConfig);

            resolveRateLimit('test-bucket', mockLog, (err, result) => {
                assert.ifError(err);
                assert.deepStrictEqual(result, cachedConfig);
                assert(getCachedConfigStub.calledOnceWith('bucket:test-bucket'));
                assert(mockLog.trace.calledWith('Rate limit config cache hit'));
                assert(getBucketStub.notCalled);
                done();
            });
        });

        it('should return cached null on cache hit for no limit', done => {
            getCachedConfigStub.returns(null);

            resolveRateLimit('test-bucket', mockLog, (err, result) => {
                assert.ifError(err);
                assert.strictEqual(result, null);
                assert(getCachedConfigStub.calledOnceWith('bucket:test-bucket'));
                assert(getBucketStub.notCalled);
                done();
            });
        });

        it('should fetch from metadata on cache miss', done => {
            getCachedConfigStub.returns(undefined);
            getBucketStub.callsFake((bucketName, log, cb) => {
                cb(null, {
                    getRateLimitConfiguration: () => null,
                });
            });

            resolveRateLimit('test-bucket', mockLog, err => {
                assert.ifError(err);
                assert(mockLog.trace.calledWith('Rate limit config cache miss'));
                assert(getBucketStub.calledOnce);
                done();
            });
        });
    });

    describe('per-bucket config resolution', () => {
        it('should resolve per-bucket config with highest priority', done => {
            getCachedConfigStub.returns(undefined);

            const mockBucketConfig = {
                getData: () => ({
                    RequestsPerSecond: {
                        Limit: 500,
                    },
                }),
            };

            getBucketStub.callsFake((bucketName, log, cb) => {
                cb(null, {
                    getRateLimitConfiguration: () => mockBucketConfig,
                });
            });

            resolveRateLimit('test-bucket', mockLog, (err, result) => {
                assert.ifError(err);
                assert.deepStrictEqual(result, {
                    limit: 500,
                    source: 'bucket',
                });
                assert(setCachedConfigStub.calledOnce);
                assert.strictEqual(setCachedConfigStub.firstCall.args[0], 'bucket:test-bucket');
                assert.deepStrictEqual(setCachedConfigStub.firstCall.args[1], {
                    limit: 500,
                    source: 'bucket',
                });
                assert(mockLog.debug.calledWith('Resolved per-bucket rate limit config'));
                done();
            });
        });

        it('should cache per-bucket config with correct TTL', done => {
            getCachedConfigStub.returns(undefined);

            const mockBucketConfig = {
                getData: () => ({
                    RequestsPerSecond: {
                        Limit: 300,
                    },
                }),
            };

            getBucketStub.callsFake((bucketName, log, cb) => {
                cb(null, {
                    getRateLimitConfiguration: () => mockBucketConfig,
                });
            });

            resolveRateLimit('test-bucket', mockLog, err => {
                assert.ifError(err);
                const ttl = setCachedConfigStub.firstCall.args[2];
                assert.strictEqual(ttl, constants.rateLimitDefaultConfigCacheTTL);
                done();
            });
        });
    });

    describe('global default config fallback', () => {
        it('should fall back to global default when no per-bucket config', done => {
            getCachedConfigStub.returns(undefined);

            getBucketStub.callsFake((bucketName, log, cb) => {
                cb(null, {
                    getRateLimitConfiguration: () => null,
                });
            });

            // Mock global config
            const originalBucket = config.rateLimiting.bucket;
            config.rateLimiting.bucket = {
                defaultConfig: { limit: 1000 },
                configCacheTTL: 30000,
            };

            resolveRateLimit('test-bucket', mockLog, (err, result) => {
                assert.ifError(err);
                assert.deepStrictEqual(result, {
                    limit: 1000,
                    source: 'global',
                });
                assert(mockLog.debug.calledWith('Using global default rate limit config'));
                assert(setCachedConfigStub.calledOnce);

                // Restore config
                config.rateLimiting.bucket = originalBucket;
                done();
            });
        });

        it('should skip global default if limit is 0', done => {
            getCachedConfigStub.returns(undefined);

            getBucketStub.callsFake((bucketName, log, cb) => {
                cb(null, {
                    getRateLimitConfiguration: () => null,
                });
            });

            // Mock global config with 0 limit
            const originalBucket = config.rateLimiting.bucket;
            config.rateLimiting.bucket = {
                defaultConfig: { limit: 0 },
            };

            resolveRateLimit('test-bucket', mockLog, (err, result) => {
                assert.ifError(err);
                assert.strictEqual(result, null);
                assert(mockLog.trace.calledWith('No rate limit configured for bucket'));

                // Restore config
                config.rateLimiting.bucket = originalBucket;
                done();
            });
        });

        it('should skip global default if undefined', done => {
            getCachedConfigStub.returns(undefined);

            getBucketStub.callsFake((bucketName, log, cb) => {
                cb(null, {
                    getRateLimitConfiguration: () => null,
                });
            });

            // Mock global config with undefined limit
            const originalBucket = config.rateLimiting.bucket;
            config.rateLimiting.bucket = {
                defaultConfig: undefined,
            };

            resolveRateLimit('test-bucket', mockLog, (err, result) => {
                assert.ifError(err);
                assert.strictEqual(result, null);

                // Restore config
                config.rateLimiting.bucket = originalBucket;
                done();
            });
        });
    });

    describe('no config case', () => {
        it('should return null when no config exists anywhere', done => {
            getCachedConfigStub.returns(undefined);

            getBucketStub.callsFake((bucketName, log, cb) => {
                cb(null, {
                    getRateLimitConfiguration: () => null,
                });
            });

            // Ensure no global config
            const originalBucket = config.rateLimiting.bucket;
            config.rateLimiting.bucket = undefined;

            resolveRateLimit('test-bucket', mockLog, (err, result) => {
                assert.ifError(err);
                assert.strictEqual(result, null);
                assert(mockLog.trace.calledWith('No rate limit configured for bucket'));

                // Restore config
                config.rateLimiting.bucket = originalBucket;
                done();
            });
        });

        it('should cache null when no config exists', done => {
            getCachedConfigStub.returns(undefined);

            getBucketStub.callsFake((bucketName, log, cb) => {
                cb(null, {
                    getRateLimitConfiguration: () => null,
                });
            });

            // Ensure no global config
            const originalBucket = config.rateLimiting.bucket;
            config.rateLimiting.bucket = undefined;

            resolveRateLimit('test-bucket', mockLog, err => {
                assert.ifError(err);
                assert(setCachedConfigStub.calledOnce);
                assert.strictEqual(setCachedConfigStub.firstCall.args[0], 'bucket:test-bucket');
                assert.strictEqual(setCachedConfigStub.firstCall.args[1], null);

                // Restore config
                config.rateLimiting.bucket = originalBucket;
                done();
            });
        });
    });

    describe('error handling', () => {
        it('should return null when bucket does not exist (NoSuchBucket)', done => {
            getCachedConfigStub.returns(undefined);

            const noSuchBucketError = { NoSuchBucket: true };
            getBucketStub.callsFake((bucketName, log, cb) => {
                cb(noSuchBucketError);
            });

            resolveRateLimit('test-bucket', mockLog, (err, result) => {
                assert.ifError(err);
                assert.strictEqual(result, null);
                assert(mockLog.trace.calledWith('Bucket does not exist, no rate limit applied'));
                assert(setCachedConfigStub.calledOnce);
                assert.strictEqual(setCachedConfigStub.firstCall.args[1], null);
                done();
            });
        });

        it('should handle metadata service errors', done => {
            getCachedConfigStub.returns(undefined);

            const testError = new Error('Metadata service unavailable');
            getBucketStub.callsFake((bucketName, log, cb) => {
                cb(testError);
            });

            resolveRateLimit('test-bucket', mockLog, err => {
                assert.strictEqual(err, testError);
                assert(mockLog.debug.calledWith('Failed to fetch bucket metadata for rate limit config'));
                assert(setCachedConfigStub.notCalled);
                done();
            });
        });
    });

    describe('TTL configuration', () => {
        it('should use custom TTL from config when available', done => {
            getCachedConfigStub.returns(undefined);

            getBucketStub.callsFake((bucketName, log, cb) => {
                cb(null, {
                    getRateLimitConfiguration: () => null,
                });
            });

            // Mock config with custom TTL
            const originalBucket = config.rateLimiting.bucket;
            config.rateLimiting.bucket = {
                configCacheTTL: 60000,
            };

            resolveRateLimit('test-bucket', mockLog, err => {
                assert.ifError(err);
                const ttl = setCachedConfigStub.firstCall.args[2];
                assert.strictEqual(ttl, 60000);

                // Restore config
                config.rateLimiting.bucket = originalBucket;
                done();
            });
        });

        it('should use default TTL constant when config TTL not set', done => {
            getCachedConfigStub.returns(undefined);

            getBucketStub.callsFake((bucketName, log, cb) => {
                cb(null, {
                    getRateLimitConfiguration: () => null,
                });
            });

            // Mock config without custom TTL
            const originalBucket = config.rateLimiting.bucket;
            config.rateLimiting.bucket = undefined;

            resolveRateLimit('test-bucket', mockLog, err => {
                assert.ifError(err);
                const ttl = setCachedConfigStub.firstCall.args[2];
                assert.strictEqual(ttl, constants.rateLimitDefaultConfigCacheTTL);

                // Restore config
                config.rateLimiting.bucket = originalBucket;
                done();
            });
        });
    });
});
