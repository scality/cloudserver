const assert = require('assert');
const sinon = require('sinon');

const { config } = require('../../../../../lib/Config');
const refillJob = require('../../../../../lib/api/apiUtils/rateLimit/refillJob');
const tokenBucket = require('../../../../../lib/api/apiUtils/rateLimit/tokenBucket');
const logger = require('../../../../../lib/utilities/logger');

describe('Token refill job', () => {
    let sandbox;
    let mockLog;

    beforeEach(() => {
        sandbox = sinon.createSandbox();
        sandbox.stub(config, 'rateLimiting').value({
            enabled: true,
            nodes: 1,
            tokenBucketBufferSize: 50,
            tokenBucketRefillThreshold: 20,
        });
        mockLog = {
            trace: sinon.stub(),
            debug: sinon.stub(),
            info: sinon.stub(),
            warn: sinon.stub(),
            error: sinon.stub(),
        };

        // Clear token buckets
        tokenBucket.getAllTokenBuckets().clear();

        // Stop any running refill job
        refillJob.stopRefillJob(logger);
    });

    afterEach(() => {
        sandbox.restore();
        refillJob.stopRefillJob(logger);
    });

    describe('refillTokenBuckets', () => {
        it('should return zero stats when no buckets exist', async () => {
            const stats = await refillJob.refillTokenBuckets(logger);

            assert.deepStrictEqual(stats, {
                checked: 0,
                refilled: 0,
            });
        });

        it('should check all active token buckets', async () => {
            // Create 3 buckets
            const bucket1 = tokenBucket.getTokenBucket('bkt', 'bucket-1', 'rps', { limit: 100 }, mockLog);
            const bucket2 = tokenBucket.getTokenBucket('bkt', 'bucket-2', 'rps', { limit: 200 }, mockLog);
            const bucket3 = tokenBucket.getTokenBucket('bkt', 'bucket-3', 'rps', { limit: 300 }, mockLog);

            // Stub refillIfNeeded to prevent actual refill
            sandbox.stub(bucket1, 'refillIfNeeded').resolves();
            sandbox.stub(bucket2, 'refillIfNeeded').resolves();
            sandbox.stub(bucket3, 'refillIfNeeded').resolves();

            const stats = await refillJob.refillTokenBuckets(logger);

            assert.strictEqual(stats.checked, 3);
            assert(bucket1.refillIfNeeded.calledOnce);
            assert(bucket2.refillIfNeeded.calledOnce);
            assert(bucket3.refillIfNeeded.calledOnce);
        });

        it('should call refillIfNeeded on each bucket', async () => {
            const bucket = tokenBucket.getTokenBucket('bkt', 'test-bucket', 'rps', { limit: 100 }, mockLog);
            const refillSpy = sandbox.stub(bucket, 'refillIfNeeded').resolves();

            await refillJob.refillTokenBuckets(logger);

            assert(refillSpy.calledOnce);
        });

        it('should handle refill errors gracefully', async () => {
            const bucket = tokenBucket.getTokenBucket('bkt', 'test-bucket', 'rps', { limit: 100 }, mockLog);
            sandbox.stub(bucket, 'refillIfNeeded').rejects(new Error('Refill failed'));

            // Should not throw
            const stats = await refillJob.refillTokenBuckets(logger);

            assert.strictEqual(stats.checked, 1);
        });

        it('should process multiple buckets in parallel', async () => {
            const bucket1 = tokenBucket.getTokenBucket('bkt', 'bucket-1', 'rps', { limit: 100 }, mockLog);
            const bucket2 = tokenBucket.getTokenBucket('bkt', 'bucket-2', 'rps', { limit: 200 }, mockLog);

            let refill1Called = false;
            let refill2Called = false;

            sandbox.stub(bucket1, 'refillIfNeeded').callsFake(async () => {
                refill1Called = true;
            });

            sandbox.stub(bucket2, 'refillIfNeeded').callsFake(async () => {
                refill2Called = true;
            });

            await refillJob.refillTokenBuckets(logger);

            assert.strictEqual(refill1Called, true);
            assert.strictEqual(refill2Called, true);
        });

        it('should wait for all refills to complete', async () => {
            const bucket1 = tokenBucket.getTokenBucket('bkt', 'bucket-1', 'rps', { limit: 100 }, mockLog);
            const bucket2 = tokenBucket.getTokenBucket('bkt', 'bucket-2', 'rps', { limit: 200 }, mockLog);

            let call1 = false;
            let call2 = false;

            sandbox.stub(bucket1, 'refillIfNeeded').callsFake(async () => {
                call1 = true;
            });

            sandbox.stub(bucket2, 'refillIfNeeded').callsFake(async () => {
                call2 = true;
            });

            await refillJob.refillTokenBuckets(logger);

            // Both should have been called
            assert.strictEqual(call1, true);
            assert.strictEqual(call2, true);
        });
    });

    describe('startRefillJob', () => {
        it('should start the refill job', () => {
            refillJob.startRefillJob(logger);
            // If it doesn't throw, it started successfully
        });

        it('should handle errors in refill loop', () => {
            sandbox.stub(refillJob, 'refillTokenBuckets').rejects(new Error('Refill error'));

            // Should not throw when starting
            refillJob.startRefillJob(logger);
        });
    });

    describe('stopRefillJob', () => {
        it('should stop the refill job', () => {
            refillJob.startRefillJob(logger);
            refillJob.stopRefillJob(logger);
            // Should not throw
        });

        it('should be idempotent (safe to call multiple times)', () => {
            refillJob.startRefillJob(logger);

            // Stop multiple times
            refillJob.stopRefillJob(logger);
            refillJob.stopRefillJob(logger);
            refillJob.stopRefillJob(logger);

            // Should not throw
        });

        it('should be safe to call when job is not running', () => {
            // Job not started
            refillJob.stopRefillJob(logger);

            // Should not throw
        });

        it('should allow restarting after stop', () => {
            // Start, stop, start again
            refillJob.startRefillJob(logger);
            refillJob.stopRefillJob(logger);
            refillJob.startRefillJob(logger);

            // Cleanup
            refillJob.stopRefillJob(logger);
        });
    });

    describe('Integration scenarios', () => {
        it('should call refillIfNeeded on buckets below threshold', async () => {
            const bucket = tokenBucket.getTokenBucket('bkt', 'test-bucket', 'rps', { limit: 100 }, mockLog);
            bucket.tokens = 10; // Below threshold (20)

            // Stub refillIfNeeded
            const refillStub = sandbox.stub(bucket, 'refillIfNeeded').resolves();

            await refillJob.refillTokenBuckets(logger);

            assert.strictEqual(refillStub.calledOnce, true);
        });

        it('should skip refill for buckets above threshold', async () => {
            const bucket = tokenBucket.getTokenBucket('bkt', 'test-bucket', 'rps', { limit: 100 }, mockLog);
            bucket.tokens = 40; // Above threshold (20)

            // refillIfNeeded is always called by the job, but it checks
            // the threshold internally and returns early without refilling
            const refillSpy = sandbox.spy(bucket, 'refillIfNeeded');

            await refillJob.refillTokenBuckets(logger);

            // refillIfNeeded was called, but tokens should be unchanged
            // (no actual refill happened since above threshold)
            assert.strictEqual(refillSpy.calledOnce, true);
            assert.strictEqual(bucket.tokens, 40);
        });

        it('should handle concurrent refills for multiple buckets', async () => {
            const bucket1 = tokenBucket.getTokenBucket('bkt', 'bucket-1', 'rps', { limit: 100 }, mockLog);
            const bucket2 = tokenBucket.getTokenBucket('bkt', 'bucket-2', 'rps', { limit: 200 }, mockLog);
            const bucket3 = tokenBucket.getTokenBucket('bkt', 'bucket-3', 'rps', { limit: 300 }, mockLog);

            // All below threshold
            bucket1.tokens = 5;
            bucket2.tokens = 10;
            bucket3.tokens = 15;

            // Stub refillIfNeeded
            const stub1 = sandbox.stub(bucket1, 'refillIfNeeded').resolves();
            const stub2 = sandbox.stub(bucket2, 'refillIfNeeded').resolves();
            const stub3 = sandbox.stub(bucket3, 'refillIfNeeded').resolves();

            await refillJob.refillTokenBuckets(logger);

            // All should have been called
            assert.strictEqual(stub1.calledOnce, true);
            assert.strictEqual(stub2.calledOnce, true);
            assert.strictEqual(stub3.calledOnce, true);
        });
    });

    describe('stopRefillJob with a tick in flight', () => {
        it('should not re-arm the timer when stopped mid-tick', async () => {
            let stopped = false;
            let callsAfterStop = 0;

            // refill takes longer than the tick interval, so a tick is
            // guaranteed to be awaiting when stopRefillJob() runs
            tokenBucket.getAllTokenBuckets().set('account:slow:rps', {
                tokens: 0,
                refillIfNeeded: async () => {
                    if (stopped) {
                        callsAfterStop++;
                    }
                    await new Promise(resolve => setTimeout(resolve, 120));
                    return false;
                },
            });

            refillJob.startRefillJob(logger, { skipUnref: true });
            await new Promise(resolve => setTimeout(resolve, 150));

            stopped = true;
            refillJob.stopRefillJob(logger);
            await new Promise(resolve => setTimeout(resolve, 500));

            assert.strictEqual(callsAfterStop, 0,
                'refill job kept running after stopRefillJob() returned');
        });

        it('should refuse to start a second concurrent job', async () => {
            let calls = 0;
            tokenBucket.getAllTokenBuckets().set('account:counted:rps', {
                tokens: 0,
                refillIfNeeded: async () => { calls++; return false; },
            });

            // a second start must not orphan the first timer and leave two
            // independent tick loops running at double the refill rate
            refillJob.startRefillJob(logger, { skipUnref: true });
            refillJob.startRefillJob(logger, { skipUnref: true });

            await new Promise(resolve => setTimeout(resolve, 450));
            refillJob.stopRefillJob(logger);

            // ~4 ticks for one loop over 450ms at a 100ms interval; two loops
            // would roughly double it
            assert.ok(calls <= 6,
                `expected a single refill loop, saw ${calls} refills in 450ms`);
        });
    });
});
