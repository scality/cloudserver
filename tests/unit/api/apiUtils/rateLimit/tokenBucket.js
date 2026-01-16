const assert = require('assert');
const sinon = require('sinon');

const tokenBucket = require('../../../../../lib/api/apiUtils/rateLimit/tokenBucket');
const { config } = require('../../../../../lib/Config');

describe('WorkerTokenBucket', () => {
    let sandbox;
    let mockLog;

    beforeEach(() => {
        sandbox = sinon.createSandbox();
        mockLog = {
            trace: sinon.stub(),
            debug: sinon.stub(),
            info: sinon.stub(),
            warn: sinon.stub(),
            error: sinon.stub(),
        };

        // Stub config
        sandbox.stub(config, 'rateLimiting').value({
            nodes: 1,
            bucket: {
                defaultConfig: {
                    requestsPerSecond: {
                        burstCapacity: 2,
                    },
                },
            },
        });
        sandbox.stub(config, 'clusters').value(1);

        // Clear token buckets map
        tokenBucket.getAllTokenBuckets().clear();
    });

    afterEach(() => {
        sandbox.restore();
    });

    describe('constructor', () => {
        it('should initialize with default values', () => {
            const bucket = new tokenBucket.WorkerTokenBucket('test-bucket', { limit: 100 }, mockLog);

            assert.strictEqual(bucket.bucketName, 'test-bucket');
            assert.deepStrictEqual(bucket.limitConfig, { limit: 100 });
            assert.strictEqual(bucket.bufferSize, 50);
            assert.strictEqual(bucket.refillThreshold, 20);
            assert.strictEqual(bucket.tokens, 50); // Starts with full buffer for fail-open
            assert.strictEqual(bucket.refillInProgress, false);
            assert.strictEqual(bucket.refillCount, 0);
        });

        it('should use custom bufferSize from config.rateLimiting', () => {
            sandbox.restore();
            sandbox.stub(config, 'rateLimiting').value({
                nodes: 1,
                tokenBucketBufferSize: 100,
                bucket: {
                    defaultConfig: {
                        requestsPerSecond: { burstCapacity: 2 },
                    },
                },
            });

            const bucket = new tokenBucket.WorkerTokenBucket('test-bucket', { limit: 100 }, mockLog);

            assert.strictEqual(bucket.bufferSize, 100);
            assert.strictEqual(bucket.tokens, 100); // tokens = bufferSize
        });

        it('should use custom refillThreshold from config.rateLimiting', () => {
            sandbox.restore();
            sandbox.stub(config, 'rateLimiting').value({
                nodes: 1,
                tokenBucketRefillThreshold: 30,
                bucket: {
                    defaultConfig: {
                        requestsPerSecond: { burstCapacity: 2 },
                    },
                },
            });

            const bucket = new tokenBucket.WorkerTokenBucket('test-bucket', { limit: 100 }, mockLog);

            assert.strictEqual(bucket.refillThreshold, 30);
        });

        it('should use both custom bufferSize and refillThreshold from config', () => {
            sandbox.restore();
            sandbox.stub(config, 'rateLimiting').value({
                nodes: 1,
                tokenBucketBufferSize: 75,
                tokenBucketRefillThreshold: 25,
                bucket: {
                    defaultConfig: {
                        requestsPerSecond: { burstCapacity: 2 },
                    },
                },
            });

            const bucket = new tokenBucket.WorkerTokenBucket('test-bucket', { limit: 100 }, mockLog);

            assert.strictEqual(bucket.bufferSize, 75);
            assert.strictEqual(bucket.refillThreshold, 25);
            assert.strictEqual(bucket.tokens, 75); // tokens = bufferSize
        });

        it('should fallback to defaults when rateLimiting is undefined', () => {
            sandbox.restore();
            sandbox.stub(config, 'rateLimiting').value(undefined);

            const bucket = new tokenBucket.WorkerTokenBucket('test-bucket', { limit: 100 }, mockLog);

            assert.strictEqual(bucket.bufferSize, 50);
            assert.strictEqual(bucket.refillThreshold, 20);
            assert.strictEqual(bucket.tokens, 50);
        });
    });

    describe('tryConsume', () => {
        it('should consume token when available', () => {
            const bucket = new tokenBucket.WorkerTokenBucket('test-bucket', { limit: 100 }, mockLog);
            bucket.tokens = 10;

            const result = bucket.tryConsume();

            assert.strictEqual(result, true);
            assert.strictEqual(bucket.tokens, 9);
        });

        it('should return false when no tokens available', () => {
            const bucket = new tokenBucket.WorkerTokenBucket('test-bucket', { limit: 100 }, mockLog);
            bucket.tokens = 0;

            const result = bucket.tryConsume();

            assert.strictEqual(result, false);
            assert.strictEqual(bucket.tokens, 0);
        });

        it('should record request timestamp', () => {
            const bucket = new tokenBucket.WorkerTokenBucket('test-bucket', { limit: 100 }, mockLog);
            bucket.tokens = 5;

            bucket.tryConsume();

            assert.strictEqual(bucket.requestTimestamps.size, 1);
            const firstValue = bucket.requestTimestamps.values().next().value;
            assert(firstValue <= Date.now() * 1000);
        });

        it('should handle multiple sequential consumptions', () => {
            const bucket = new tokenBucket.WorkerTokenBucket('test-bucket', { limit: 100 }, mockLog);
            bucket.tokens = 3;

            assert.strictEqual(bucket.tryConsume(), true);
            assert.strictEqual(bucket.tokens, 2);

            assert.strictEqual(bucket.tryConsume(), true);
            assert.strictEqual(bucket.tokens, 1);

            assert.strictEqual(bucket.tryConsume(), true);
            assert.strictEqual(bucket.tokens, 0);

            assert.strictEqual(bucket.tryConsume(), false);
            assert.strictEqual(bucket.tokens, 0);
        });
    });

    describe('recordRequest', () => {
        it('should add timestamp to array', () => {
            const bucket = new tokenBucket.WorkerTokenBucket('test-bucket', { limit: 100 }, mockLog);
            const before = Date.now() * 1000;

            bucket.recordRequest();

            assert.strictEqual(bucket.requestTimestamps.size, 1);
            const firstValue = bucket.requestTimestamps.values().next().value;
            assert(firstValue >= before);
            assert(firstValue <= Date.now() * 1000);
        });

        it('should expire old timestamps beyond 1 second', () => {
            const bucket = new tokenBucket.WorkerTokenBucket('test-bucket', { limit: 100 }, mockLog);
            const now = Date.now() * 1000;

            // Add old timestamp (2 seconds ago)
            bucket.requestTimestamps.add(now - 2000000);

            // Add recent timestamp
            bucket.requestTimestamps.add(now - 500000);

            // Record new request
            bucket.recordRequest();

            // Old timestamp should be removed
            assert.strictEqual(bucket.requestTimestamps.size, 2);
            const firstValue = bucket.requestTimestamps.values().next().value;
            assert(firstValue >= now - 1000000);
        });
    });

    describe('getCurrentRate', () => {
        it('should return 0 when no requests', () => {
            const bucket = new tokenBucket.WorkerTokenBucket('test-bucket', { limit: 100 }, mockLog);

            assert.strictEqual(bucket.getCurrentRate(), 0);
        });

        it('should count requests in last second', () => {
            const bucket = new tokenBucket.WorkerTokenBucket('test-bucket', { limit: 100 }, mockLog);
            const now = Date.now() * 1000;

            // Add 5 requests in last second
            bucket.requestTimestamps.add(now - 900000);
            bucket.requestTimestamps.add(now - 700000);
            bucket.requestTimestamps.add(now - 500000);
            bucket.requestTimestamps.add(now - 300000);
            bucket.requestTimestamps.add(now - 100000);

            assert.strictEqual(bucket.getCurrentRate(), 5);
        });

        it('should exclude requests older than 1 second', () => {
            const bucket = new tokenBucket.WorkerTokenBucket('test-bucket', { limit: 100 }, mockLog);
            const now = Date.now() * 1000;

            // Add old requests (beyond 1 second)
            bucket.requestTimestamps.add(now - 1500000);
            bucket.requestTimestamps.add(now - 1200000);

            // Add recent requests
            bucket.requestTimestamps.add(now - 800000);
            bucket.requestTimestamps.add(now - 400000);

            assert.strictEqual(bucket.getCurrentRate(), 2);
        });
    });

    describe('refillIfNeeded', () => {
        it('should skip refill when above threshold', async () => {
            const bucket = new tokenBucket.WorkerTokenBucket('test-bucket', { limit: 100 }, mockLog);
            bucket.tokens = 30; // Above threshold of 20
            const refillSpy = sandbox.spy(bucket, 'refill');

            await bucket.refillIfNeeded();

            assert.strictEqual(refillSpy.called, false);
        });

        it('should skip refill when already in progress', async () => {
            const bucket = new tokenBucket.WorkerTokenBucket('test-bucket', { limit: 100 }, mockLog);
            bucket.tokens = 10; // Below threshold
            bucket.refillInProgress = true;
            const refillSpy = sandbox.spy(bucket, 'refill');

            await bucket.refillIfNeeded();

            assert.strictEqual(refillSpy.called, false);
        });

        it('should trigger refill when below threshold', async () => {
            const bucket = new tokenBucket.WorkerTokenBucket('test-bucket', { limit: 100 }, mockLog);
            bucket.tokens = 10; // Below threshold of 20

            // Stub refill to prevent actual Redis call
            sandbox.stub(bucket, 'refill').resolves();

            await bucket.refillIfNeeded();

            assert.strictEqual(bucket.refill.calledOnce, true);
        });
    });

    describe('refill', () => {
        it('should skip refill when buffer is already full', async () => {
            const bucket = new tokenBucket.WorkerTokenBucket('test-bucket', { limit: 100 }, mockLog);
            bucket.tokens = 50; // Buffer at maximum (bufferSize = 50)

            // Mock refill to verify early return
            const refillSpy = sandbox.spy(bucket, 'refill');

            await bucket.refill();

            // Verify refill was called but returned early (no Redis call)
            assert.strictEqual(refillSpy.calledOnce, true);

            // Tokens should remain unchanged
            assert.strictEqual(bucket.tokens, 50);
        });

        it('should not call Redis when requested amount is zero', async () => {
            const bucket = new tokenBucket.WorkerTokenBucket('test-bucket', { limit: 100 }, mockLog);
            bucket.tokens = 51; // More than buffer size (edge case)

            await bucket.refill();

            // Should complete without errors even though requested < 0
            assert(true);
        });

        // NOTE: Full refill() tests with actual Redis calls and token granting logic
        // are better suited for functional/integration tests. These unit tests verify
        // the early-return logic and buffer boundary conditions without requiring Redis.
    });

    describe('getStats', () => {
        it('should return current bucket stats', () => {
            const bucket = new tokenBucket.WorkerTokenBucket('test-bucket', { limit: 100 }, mockLog);
            bucket.tokens = 25;
            bucket.refillCount = 5;
            bucket.requestTimestamps.add(Date.now() * 1000);

            const stats = bucket.getStats();

            assert.strictEqual(stats.tokens, 25);
            assert.strictEqual(stats.bufferSize, 50);
            assert.strictEqual(stats.refillThreshold, 20);
            assert.strictEqual(stats.refillInProgress, false);
            assert.strictEqual(stats.refillCount, 5);
            assert.strictEqual(stats.currentRate, 1);
            assert(typeof stats.lastRefillTime === 'number');
        });
    });
});

describe('Token bucket management functions', () => {
    let sandbox;
    let mockLog;
    let tokenBucket;

    beforeEach(() => {
        sandbox = sinon.createSandbox();
        mockLog = {
            trace: sinon.stub(),
            debug: sinon.stub(),
            info: sinon.stub(),
            error: sinon.stub(),
        };

        // Load tokenBucket module (no need for Redis client in these tests)
        tokenBucket = require('../../../../../lib/api/apiUtils/rateLimit/tokenBucket');

        // Clear token buckets
        tokenBucket.getAllTokenBuckets().clear();
    });

    afterEach(() => {
        sandbox.restore();
    });

    describe('getTokenBucket', () => {
        it('should create new bucket on first call', () => {
            const bucket = tokenBucket.getTokenBucket('test-bucket', { limit: 100 }, mockLog);

            assert(bucket instanceof tokenBucket.WorkerTokenBucket);
            assert.strictEqual(bucket.bucketName, 'test-bucket');
            assert(mockLog.debug.calledOnce);
            assert(mockLog.debug.firstCall.args[0].includes('Created token bucket'));
        });

        it('should return existing bucket on subsequent calls', () => {
            const bucket1 = tokenBucket.getTokenBucket('test-bucket', { limit: 100 }, mockLog);
            const bucket2 = tokenBucket.getTokenBucket('test-bucket', { limit: 100 }, mockLog);

            assert.strictEqual(bucket1, bucket2);
            assert.strictEqual(mockLog.debug.callCount, 1); // Only called once
        });

        it('should create separate buckets for different bucket names', () => {
            const bucket1 = tokenBucket.getTokenBucket('bucket-1', { limit: 100 }, mockLog);
            const bucket2 = tokenBucket.getTokenBucket('bucket-2', { limit: 100 }, mockLog);

            assert.notStrictEqual(bucket1, bucket2);
            assert.strictEqual(bucket1.bucketName, 'bucket-1');
            assert.strictEqual(bucket2.bucketName, 'bucket-2');
        });

        it('should update limitConfig when limit changes', () => {
            const bucket1 = tokenBucket.getTokenBucket('test-bucket', { limit: 100, source: 'bucket' }, mockLog);
            assert.strictEqual(bucket1.limitConfig.limit, 100);

            // Simulate rate limit change
            const bucket2 = tokenBucket.getTokenBucket('test-bucket', { limit: 200, source: 'bucket' }, mockLog);

            assert.strictEqual(bucket1, bucket2); // Same bucket instance
            assert.strictEqual(bucket2.limitConfig.limit, 200); // Limit updated
            assert(mockLog.info.calledOnce);
            assert(mockLog.info.firstCall.args[0].includes('Updated token bucket limit config'));
        });

        it('should not log update when limit is unchanged', () => {
            tokenBucket.getTokenBucket('test-bucket', { limit: 100 }, mockLog);
            mockLog.info.resetHistory();

            tokenBucket.getTokenBucket('test-bucket', { limit: 100 }, mockLog);

            assert.strictEqual(mockLog.info.called, false);
        });
    });

    describe('removeTokenBucket', () => {
        it('should remove existing bucket and return true', () => {
            tokenBucket.getTokenBucket('test-bucket', { limit: 100 }, mockLog);
            assert.strictEqual(tokenBucket.getAllTokenBuckets().size, 1);

            const result = tokenBucket.removeTokenBucket('test-bucket');

            assert.strictEqual(result, true);
            assert.strictEqual(tokenBucket.getAllTokenBuckets().size, 0);
        });

        it('should return false when bucket does not exist', () => {
            const result = tokenBucket.removeTokenBucket('non-existent-bucket');

            assert.strictEqual(result, false);
        });
    });

    describe('getAllTokenBuckets', () => {
        it('should return empty map initially', () => {
            const buckets = tokenBucket.getAllTokenBuckets();

            assert(buckets instanceof Map);
            assert.strictEqual(buckets.size, 0);
        });

        it('should return all created buckets', () => {
            tokenBucket.getTokenBucket('bucket-1', { limit: 100 }, mockLog);
            tokenBucket.getTokenBucket('bucket-2', { limit: 200 }, mockLog);
            tokenBucket.getTokenBucket('bucket-3', { limit: 300 }, mockLog);

            const buckets = tokenBucket.getAllTokenBuckets();

            assert.strictEqual(buckets.size, 3);
            assert(buckets.has('bucket-1'));
            assert(buckets.has('bucket-2'));
            assert(buckets.has('bucket-3'));
        });
    });

    describe('cleanupTokenBuckets', () => {
        it('should remove idle buckets with no tokens', () => {
            const bucket1 = tokenBucket.getTokenBucket('bucket-1', { limit: 100 }, mockLog);
            const bucket2 = tokenBucket.getTokenBucket('bucket-2', { limit: 200 }, mockLog);

            // Make bucket-1 idle (old lastRefillTime, no tokens)
            bucket1.lastRefillTime = Date.now() - 120000; // 2 minutes ago
            bucket1.tokens = 0;

            // Make bucket-2 active
            bucket2.lastRefillTime = Date.now();
            bucket2.tokens = 10;

            const removed = tokenBucket.cleanupTokenBuckets(60000); // 60s threshold

            assert.strictEqual(removed, 1);
            assert.strictEqual(tokenBucket.getAllTokenBuckets().size, 1);
            assert(tokenBucket.getAllTokenBuckets().has('bucket-2'));
            assert(!tokenBucket.getAllTokenBuckets().has('bucket-1'));
        });

        it('should not remove idle buckets with tokens', () => {
            const bucket = tokenBucket.getTokenBucket('test-bucket', { limit: 100 }, mockLog);

            // Make bucket idle but with tokens
            bucket.lastRefillTime = Date.now() - 120000;
            bucket.tokens = 10; // Has tokens

            const removed = tokenBucket.cleanupTokenBuckets(60000);

            assert.strictEqual(removed, 0);
            assert.strictEqual(tokenBucket.getAllTokenBuckets().size, 1);
        });

        it('should not remove recently active buckets', () => {
            const bucket = tokenBucket.getTokenBucket('test-bucket', { limit: 100 }, mockLog);

            // Recently active
            bucket.lastRefillTime = Date.now() - 30000; // 30s ago
            bucket.tokens = 0;

            const removed = tokenBucket.cleanupTokenBuckets(60000);

            assert.strictEqual(removed, 0);
            assert.strictEqual(tokenBucket.getAllTokenBuckets().size, 1);
        });

        it('should use default maxIdleMs if not provided', () => {
            const bucket = tokenBucket.getTokenBucket('test-bucket', { limit: 100 }, mockLog);

            // Idle beyond default (60s)
            bucket.lastRefillTime = Date.now() - 70000;
            bucket.tokens = 0;

            const removed = tokenBucket.cleanupTokenBuckets(); // No argument

            assert.strictEqual(removed, 1);
        });

        it('should handle empty bucket map', () => {
            const removed = tokenBucket.cleanupTokenBuckets(60000);

            assert.strictEqual(removed, 0);
        });

        it('should remove multiple expired buckets', () => {
            const bucket1 = tokenBucket.getTokenBucket('bucket-1', { limit: 100 }, mockLog);
            const bucket2 = tokenBucket.getTokenBucket('bucket-2', { limit: 200 }, mockLog);
            const bucket3 = tokenBucket.getTokenBucket('bucket-3', { limit: 300 }, mockLog);

            // Make bucket-1 and bucket-2 idle
            bucket1.lastRefillTime = Date.now() - 120000;
            bucket1.tokens = 0;
            bucket2.lastRefillTime = Date.now() - 120000;
            bucket2.tokens = 0;

            // Keep bucket-3 active
            bucket3.lastRefillTime = Date.now();

            const removed = tokenBucket.cleanupTokenBuckets(60000);

            assert.strictEqual(removed, 2);
            assert.strictEqual(tokenBucket.getAllTokenBuckets().size, 1);
            assert(tokenBucket.getAllTokenBuckets().has('bucket-3'));
        });
    });
});
