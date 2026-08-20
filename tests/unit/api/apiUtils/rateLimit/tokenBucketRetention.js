const assert = require('assert');
const sinon = require('sinon');

const tokenBucket = require('../../../../../lib/api/apiUtils/rateLimit/tokenBucket');
const { config } = require('../../../../../lib/Config');

function makeLog() {
    return {
        trace: sinon.stub(),
        debug: sinon.stub(),
        info: sinon.stub(),
        warn: sinon.stub(),
        error: sinon.stub(),
    };
}

function logCallCount(log) {
    return log.trace.callCount + log.debug.callCount + log.info.callCount
        + log.warn.callCount + log.error.callCount;
}

/**
 * Regression tests: token buckets used to retain the request logger that
 * created them, and the refill job logged through it forever - werelogs
 * buffers those entries until an error-level write, so memory grew per
 * resource until process restart.
 */
describe('rate limit token bucket retention', () => {
    let sandbox;

    beforeEach(() => {
        sandbox = sinon.createSandbox();
        sandbox.stub(config, 'rateLimiting').value({
            enabled: true,
            nodes: 1,
            tokenBucketBufferSize: 50,
            tokenBucketRefillThreshold: 20,
        });
        tokenBucket.getAllTokenBuckets().clear();
    });

    afterEach(() => {
        sandbox.restore();
        tokenBucket.getAllTokenBuckets().clear();
    });

    it('should not retain the request logger that created the bucket', () => {
        const requestLog = makeLog();

        const bucket = tokenBucket.getTokenBucket(
            'account', 'acct-retention-1', 'rps', { limit: 60, burstCapacity: 1000 }, requestLog);

        const retained = Object.keys(bucket).filter(key => bucket[key] === requestLog);

        assert.deepStrictEqual(retained, [],
            `token bucket must not hold the per-request logger (held on: ${retained.join(', ')})`);
    });

    it('should log refill activity to the caller-supplied logger, not the creating request logger', async () => {
        const requestLog = makeLog();
        const jobLog = makeLog();

        const bucket = tokenBucket.getTokenBucket(
            'account', 'acct-retention-2', 'rps', { limit: 60, burstCapacity: 1000 }, requestLog);

        // ignore the "Created token bucket" line, which is legitimately
        // request-scoped and dies with the request
        requestLog.debug.resetHistory();

        bucket.tokens = 0; // below refill threshold, forces a refill
        await bucket.refillIfNeeded(jobLog);

        assert.strictEqual(logCallCount(requestLog), 0,
            'refill must not log through the request logger that created the bucket');
        assert.ok(logCallCount(jobLog) > 0,
            'refill must log through the logger supplied by the refill job');
    });

    it('should evict buckets idle longer than maxIdleMs even when they still hold tokens', () => {
        const requestLog = makeLog();

        const bucket = tokenBucket.getTokenBucket(
            'account', 'acct-retention-3', 'rps', { limit: 60, burstCapacity: 1000 }, requestLog);

        bucket.lastRefillTime = Date.now() - 120000;

        const removed = tokenBucket.cleanupTokenBuckets(60000);

        assert.strictEqual(removed, 1, 'idle bucket must be evicted even with a full token buffer');
        assert.strictEqual(tokenBucket.getAllTokenBuckets().size, 0);
    });
});
