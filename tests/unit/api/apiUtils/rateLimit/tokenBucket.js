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

        sandbox.stub(config, 'rateLimiting').value({
            nodes: 1,
            tokenBucketBufferSize: 50,
            tokenBucketRefillThreshold: 20,
        });

        tokenBucket.getAllTokenBuckets().clear();
    });

    afterEach(() => {
        sandbox.restore();
    });

    describe('constructor', () => {
        it('should initialize with correct values from config', () => {
            const bucket = new tokenBucket.WorkerTokenBucket('bucket', 'test-bucket', 'rps', { limit: 100 }, mockLog);

            assert.strictEqual(bucket.resourceClass, 'bucket');
            assert.strictEqual(bucket.resourceId, 'test-bucket');
            assert.strictEqual(bucket.measure, 'rps');
            assert.deepStrictEqual(bucket.limitConfig, { limit: 100 });
            assert.strictEqual(bucket.bufferSize, 50);
            assert.strictEqual(bucket.refillThreshold, 20);
            assert.strictEqual(bucket.tokens, 50); // Starts with full buffer for fail-open
        });

        it('should use custom bufferSize from config.rateLimiting', () => {
            sandbox.restore();
            sandbox.stub(config, 'rateLimiting').value({
                nodes: 1,
                tokenBucketBufferSize: 100,
                tokenBucketRefillThreshold: 20,
            });

            const bucket = new tokenBucket.WorkerTokenBucket('bucket', 'test-bucket', 'rps', { limit: 100 }, mockLog);

            assert.strictEqual(bucket.bufferSize, 100);
            assert.strictEqual(bucket.tokens, 100);
        });

        it('should use custom refillThreshold from config.rateLimiting', () => {
            sandbox.restore();
            sandbox.stub(config, 'rateLimiting').value({
                nodes: 1,
                tokenBucketBufferSize: 50,
                tokenBucketRefillThreshold: 30,
            });

            const bucket = new tokenBucket.WorkerTokenBucket('bucket', 'test-bucket', 'rps', { limit: 100 }, mockLog);

            assert.strictEqual(bucket.refillThreshold, 30);
        });

        it('should use both custom bufferSize and refillThreshold from config', () => {
            sandbox.restore();
            sandbox.stub(config, 'rateLimiting').value({
                nodes: 1,
                tokenBucketBufferSize: 75,
                tokenBucketRefillThreshold: 25,
            });

            const bucket = new tokenBucket.WorkerTokenBucket('bucket', 'test-bucket', 'rps', { limit: 100 }, mockLog);

            assert.strictEqual(bucket.bufferSize, 75);
            assert.strictEqual(bucket.refillThreshold, 25);
            assert.strictEqual(bucket.tokens, 75);
        });
    });

    describe('tryConsume', () => {
        it('should consume token when available', () => {
            const bucket = new tokenBucket.WorkerTokenBucket('bucket', 'test-bucket', 'rps', { limit: 100 }, mockLog);
            bucket.tokens = 10;

            const result = bucket.tryConsume();

            assert.strictEqual(result, true);
            assert.strictEqual(bucket.tokens, 9);
        });

        it('should return false when no tokens available', () => {
            const bucket = new tokenBucket.WorkerTokenBucket('bucket', 'test-bucket', 'rps', { limit: 100 }, mockLog);
            bucket.tokens = 0;

            const result = bucket.tryConsume();

            assert.strictEqual(result, false);
            assert.strictEqual(bucket.tokens, 0);
        });

        it('should handle multiple sequential consumptions', () => {
            const bucket = new tokenBucket.WorkerTokenBucket('bucket', 'test-bucket', 'rps', { limit: 100 }, mockLog);
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

    describe('hasCapacity', () => {
        it('should return true when tokens are available', () => {
            const bucket = new tokenBucket.WorkerTokenBucket('bucket', 'test-bucket', 'rps', { limit: 100 }, mockLog);
            bucket.tokens = 1;

            assert.strictEqual(bucket.hasCapacity(), true);
        });

        it('should return false when no tokens available', () => {
            const bucket = new tokenBucket.WorkerTokenBucket('bucket', 'test-bucket', 'rps', { limit: 100 }, mockLog);
            bucket.tokens = 0;

            assert.strictEqual(bucket.hasCapacity(), false);
        });
    });

    describe('updateLimit', () => {
        it('should update limitConfig and interval when limit changes', () => {
            const bucket = new tokenBucket.WorkerTokenBucket(
                'bucket', 'test-bucket', 'rps', { limit: 100, burstCapacity: 1000 }, mockLog);
            const oldInterval = bucket.interval;

            const result = bucket.updateLimit({ limit: 200, burstCapacity: 1000 });

            assert.strictEqual(result.updated, true);
            assert.deepStrictEqual(result.oldConfig, { limit: 100, burstCapacity: 1000 });
            assert.strictEqual(bucket.limitConfig.limit, 200);
            assert.notStrictEqual(bucket.interval, oldInterval);
        });

        it('should update limitConfig when burstCapacity changes', () => {
            const bucket = new tokenBucket.WorkerTokenBucket(
                'bucket', 'test-bucket', 'rps', { limit: 100, burstCapacity: 1000 }, mockLog);

            const result = bucket.updateLimit({ limit: 100, burstCapacity: 2000 });

            assert.strictEqual(result.updated, true);
            assert.strictEqual(bucket.limitConfig.burstCapacity, 2000);
        });

        it('should return updated: false when config is unchanged', () => {
            const bucket = new tokenBucket.WorkerTokenBucket(
                'bucket', 'test-bucket', 'rps', { limit: 100, burstCapacity: 1000 }, mockLog);

            const result = bucket.updateLimit({ limit: 100, burstCapacity: 1000 });

            assert.strictEqual(result.updated, false);
        });
    });

    describe('refillIfNeeded', () => {
        it('should skip refill when above threshold', async () => {
            const bucket = new tokenBucket.WorkerTokenBucket(
                'bucket', 'test-bucket', 'rps', { limit: 100 }, mockLog);
            bucket.tokens = 30; // Above threshold of 20

            await bucket.refillIfNeeded();

            // refillInProgress was never set (no refill attempted)
            assert.ok(!bucket.refillInProgress);
        });

        it('should skip refill when already in progress', async () => {
            const bucket = new tokenBucket.WorkerTokenBucket(
                'bucket', 'test-bucket', 'rps', { limit: 100 }, mockLog);
            bucket.tokens = 10; // Below threshold
            bucket.refillInProgress = true;

            await bucket.refillIfNeeded();

            // Still true — function returned early without clearing it
            assert.strictEqual(bucket.refillInProgress, true);
        });

        it('should trigger refill when below threshold', async () => {
            const bucket = new tokenBucket.WorkerTokenBucket(
                'bucket', 'test-bucket', 'rps', { limit: 100, burstCapacity: 1000 }, mockLog);
            bucket.tokens = 10; // Below threshold of 20

            await bucket.refillIfNeeded();

            // refillInProgress is cleared in finally block regardless of outcome
            assert.strictEqual(bucket.refillInProgress, false);
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

        const { config } = require('../../../../../lib/Config');
        sandbox.stub(config, 'rateLimiting').value({
            nodes: 1,
            tokenBucketBufferSize: 50,
            tokenBucketRefillThreshold: 20,
        });

        tokenBucket = require('../../../../../lib/api/apiUtils/rateLimit/tokenBucket');
        tokenBucket.getAllTokenBuckets().clear();
    });

    afterEach(() => {
        sandbox.restore();
    });

    describe('getTokenBucket', () => {
        it('should create new bucket on first call', () => {
            const bucket = tokenBucket.getTokenBucket('bucket', 'test-bucket', 'rps', { limit: 100 }, mockLog);

            assert(bucket instanceof tokenBucket.WorkerTokenBucket);
            assert.strictEqual(bucket.resourceClass, 'bucket');
            assert.strictEqual(bucket.resourceId, 'test-bucket');
            assert.strictEqual(bucket.measure, 'rps');
            assert(mockLog.debug.calledOnce);
            assert(mockLog.debug.firstCall.args[0].includes('Created token bucket'));
        });

        it('should return existing bucket on subsequent calls', () => {
            const bucket1 = tokenBucket.getTokenBucket('bucket', 'test-bucket', 'rps', { limit: 100 }, mockLog);
            const bucket2 = tokenBucket.getTokenBucket('bucket', 'test-bucket', 'rps', { limit: 100 }, mockLog);

            assert.strictEqual(bucket1, bucket2);
            assert.strictEqual(mockLog.debug.callCount, 1);
        });

        it('should create separate buckets for different resource IDs', () => {
            const bucket1 = tokenBucket.getTokenBucket('bucket', 'bucket-1', 'rps', { limit: 100 }, mockLog);
            const bucket2 = tokenBucket.getTokenBucket('bucket', 'bucket-2', 'rps', { limit: 100 }, mockLog);

            assert.notStrictEqual(bucket1, bucket2);
            assert.strictEqual(bucket1.resourceId, 'bucket-1');
            assert.strictEqual(bucket2.resourceId, 'bucket-2');
        });

        it('should create separate buckets for different measures', () => {
            const bucket1 = tokenBucket.getTokenBucket('bucket', 'test-bucket', 'rps', { limit: 100 }, mockLog);
            const bucket2 = tokenBucket.getTokenBucket('bucket', 'test-bucket', 'bps', { limit: 100 }, mockLog);

            assert.notStrictEqual(bucket1, bucket2);
            assert.strictEqual(bucket1.measure, 'rps');
            assert.strictEqual(bucket2.measure, 'bps');
        });

        it('should create separate buckets for different resource classes', () => {
            const bucket1 = tokenBucket.getTokenBucket('bucket', 'test', 'rps', { limit: 100 }, mockLog);
            const bucket2 = tokenBucket.getTokenBucket('account', 'test', 'rps', { limit: 100 }, mockLog);

            assert.notStrictEqual(bucket1, bucket2);
            assert.strictEqual(bucket1.resourceClass, 'bucket');
            assert.strictEqual(bucket2.resourceClass, 'account');
        });

        it('should update limitConfig when limit changes', () => {
            const bucket1 = tokenBucket.getTokenBucket(
                'bucket', 'test-bucket', 'rps', { limit: 100, source: 'bucket' }, mockLog);
            assert.strictEqual(bucket1.limitConfig.limit, 100);

            const bucket2 = tokenBucket.getTokenBucket(
                'bucket', 'test-bucket', 'rps', { limit: 200, source: 'bucket' }, mockLog);

            assert.strictEqual(bucket1, bucket2);
            assert.strictEqual(bucket2.limitConfig.limit, 200);
            assert(mockLog.info.calledOnce);
            assert(mockLog.info.firstCall.args[0].includes('Updated token bucket limit config'));
        });

        it('should not log update when limit is unchanged', () => {
            tokenBucket.getTokenBucket('bucket', 'test-bucket', 'rps', { limit: 100 }, mockLog);
            mockLog.info.resetHistory();

            tokenBucket.getTokenBucket('bucket', 'test-bucket', 'rps', { limit: 100 }, mockLog);

            assert.strictEqual(mockLog.info.called, false);
        });
    });

    describe('removeTokenBucket', () => {
        it('should remove existing bucket and return true', () => {
            tokenBucket.getTokenBucket('bucket', 'test-bucket', 'rps', { limit: 100 }, mockLog);
            assert.strictEqual(tokenBucket.getAllTokenBuckets().size, 1);

            const result = tokenBucket.removeTokenBucket('bucket', 'test-bucket', 'rps');

            assert.strictEqual(result, true);
            assert.strictEqual(tokenBucket.getAllTokenBuckets().size, 0);
        });

        it('should return false when bucket does not exist', () => {
            const result = tokenBucket.removeTokenBucket('bucket', 'non-existent-bucket', 'rps');

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
            tokenBucket.getTokenBucket('bucket', 'bucket-1', 'rps', { limit: 100 }, mockLog);
            tokenBucket.getTokenBucket('bucket', 'bucket-2', 'rps', { limit: 200 }, mockLog);
            tokenBucket.getTokenBucket('bucket', 'bucket-3', 'rps', { limit: 300 }, mockLog);

            const buckets = tokenBucket.getAllTokenBuckets();

            assert.strictEqual(buckets.size, 3);
            assert(buckets.has('bucket:bucket-1:rps'));
            assert(buckets.has('bucket:bucket-2:rps'));
            assert(buckets.has('bucket:bucket-3:rps'));
        });
    });

    describe('cleanupTokenBuckets', () => {
        it('should remove idle buckets with no tokens', () => {
            const bucket1 = tokenBucket.getTokenBucket('bucket', 'bucket-1', 'rps', { limit: 100 }, mockLog);
            const bucket2 = tokenBucket.getTokenBucket('bucket', 'bucket-2', 'rps', { limit: 200 }, mockLog);

            bucket1.lastRefillTime = Date.now() - 120000;
            bucket1.tokens = 0;

            bucket2.lastRefillTime = Date.now();
            bucket2.tokens = 10;

            const removed = tokenBucket.cleanupTokenBuckets(60000);

            assert.strictEqual(removed, 1);
            assert.strictEqual(tokenBucket.getAllTokenBuckets().size, 1);
            assert(tokenBucket.getAllTokenBuckets().has('bucket:bucket-2:rps'));
            assert(!tokenBucket.getAllTokenBuckets().has('bucket:bucket-1:rps'));
        });

        it('should not remove idle buckets with tokens', () => {
            const bucket = tokenBucket.getTokenBucket('bucket', 'test-bucket', 'rps', { limit: 100 }, mockLog);

            bucket.lastRefillTime = Date.now() - 120000;
            bucket.tokens = 10;

            const removed = tokenBucket.cleanupTokenBuckets(60000);

            assert.strictEqual(removed, 0);
            assert.strictEqual(tokenBucket.getAllTokenBuckets().size, 1);
        });

        it('should not remove recently active buckets', () => {
            const bucket = tokenBucket.getTokenBucket('bucket', 'test-bucket', 'rps', { limit: 100 }, mockLog);

            bucket.lastRefillTime = Date.now() - 30000;
            bucket.tokens = 0;

            const removed = tokenBucket.cleanupTokenBuckets(60000);

            assert.strictEqual(removed, 0);
            assert.strictEqual(tokenBucket.getAllTokenBuckets().size, 1);
        });

        it('should use default maxIdleMs if not provided', () => {
            const bucket = tokenBucket.getTokenBucket('bucket', 'test-bucket', 'rps', { limit: 100 }, mockLog);

            bucket.lastRefillTime = Date.now() - 70000;
            bucket.tokens = 0;

            const removed = tokenBucket.cleanupTokenBuckets();

            assert.strictEqual(removed, 1);
        });

        it('should handle empty bucket map', () => {
            const removed = tokenBucket.cleanupTokenBuckets(60000);

            assert.strictEqual(removed, 0);
        });

        it('should remove multiple expired buckets', () => {
            const bucket1 = tokenBucket.getTokenBucket('bucket', 'bucket-1', 'rps', { limit: 100 }, mockLog);
            const bucket2 = tokenBucket.getTokenBucket('bucket', 'bucket-2', 'rps', { limit: 200 }, mockLog);
            const bucket3 = tokenBucket.getTokenBucket('bucket', 'bucket-3', 'rps', { limit: 300 }, mockLog);

            bucket1.lastRefillTime = Date.now() - 120000;
            bucket1.tokens = 0;
            bucket2.lastRefillTime = Date.now() - 120000;
            bucket2.tokens = 0;

            bucket3.lastRefillTime = Date.now();

            const removed = tokenBucket.cleanupTokenBuckets(60000);

            assert.strictEqual(removed, 2);
            assert.strictEqual(tokenBucket.getAllTokenBuckets().size, 1);
            assert(tokenBucket.getAllTokenBuckets().has('bucket:bucket-3:rps'));
        });
    });
});
