const assert = require('assert');
const sinon = require('sinon');

const refillJob = require('../../../../../lib/api/apiUtils/rateLimit/refillJob');
const tokenBucket = require('../../../../../lib/api/apiUtils/rateLimit/tokenBucket');
const logger = require('../../../../../lib/utilities/logger');

describe('Token refill job', () => {
    let sandbox;

    beforeEach(() => {
        sandbox = sinon.createSandbox();

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
            const mockLog = {
                trace: sinon.stub(),
                debug: sinon.stub(),
                error: sinon.stub(),
            };

            // Create 3 buckets
            const bucket1 = tokenBucket.getTokenBucket('bucket-1', { limit: 100 }, mockLog);
            const bucket2 = tokenBucket.getTokenBucket('bucket-2', { limit: 200 }, mockLog);
            const bucket3 = tokenBucket.getTokenBucket('bucket-3', { limit: 300 }, mockLog);

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
            const mockLog = {
                trace: sinon.stub(),
                debug: sinon.stub(),
                error: sinon.stub(),
            };

            const bucket = tokenBucket.getTokenBucket('test-bucket', { limit: 100 }, mockLog);
            const refillSpy = sandbox.stub(bucket, 'refillIfNeeded').resolves();

            await refillJob.refillTokenBuckets(logger);

            assert(refillSpy.calledOnce);
        });

        it('should handle refill errors gracefully', async () => {
            const mockLog = {
                trace: sinon.stub(),
                debug: sinon.stub(),
                error: sinon.stub(),
            };

            const bucket = tokenBucket.getTokenBucket('test-bucket', { limit: 100 }, mockLog);
            sandbox.stub(bucket, 'refillIfNeeded').rejects(new Error('Refill failed'));

            // Should not throw
            const stats = await refillJob.refillTokenBuckets(logger);

            assert.strictEqual(stats.checked, 1);
        });

        it('should process multiple buckets in parallel', async () => {
            const mockLog = {
                trace: sinon.stub(),
                debug: sinon.stub(),
                error: sinon.stub(),
            };

            const bucket1 = tokenBucket.getTokenBucket('bucket-1', { limit: 100 }, mockLog);
            const bucket2 = tokenBucket.getTokenBucket('bucket-2', { limit: 200 }, mockLog);

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
            const mockLog = {
                trace: sinon.stub(),
                debug: sinon.stub(),
                error: sinon.stub(),
            };

            const bucket1 = tokenBucket.getTokenBucket('bucket-1', { limit: 100 }, mockLog);
            const bucket2 = tokenBucket.getTokenBucket('bucket-2', { limit: 200 }, mockLog);

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
            const mockLog = {
                trace: sinon.stub(),
                debug: sinon.stub(),
                error: sinon.stub(),
            };

            const bucket = tokenBucket.getTokenBucket('test-bucket', { limit: 100 }, mockLog);
            bucket.tokens = 10; // Below threshold (20)

            // Stub refillIfNeeded
            const refillStub = sandbox.stub(bucket, 'refillIfNeeded').resolves();

            await refillJob.refillTokenBuckets(logger);

            assert.strictEqual(refillStub.calledOnce, true);
        });

        it('should skip refill for buckets above threshold', async () => {
            const mockLog = {
                trace: sinon.stub(),
                debug: sinon.stub(),
                error: sinon.stub(),
            };

            const bucket = tokenBucket.getTokenBucket('test-bucket', { limit: 100 }, mockLog);
            bucket.tokens = 40; // Above threshold (20)

            const refillSpy = sandbox.spy(bucket, 'refill');

            await refillJob.refillTokenBuckets(logger);

            // Refill should not have been called
            assert.strictEqual(refillSpy.called, false);
        });

        it('should handle concurrent refills for multiple buckets', async () => {
            const mockLog = {
                trace: sinon.stub(),
                debug: sinon.stub(),
                error: sinon.stub(),
            };

            const bucket1 = tokenBucket.getTokenBucket('bucket-1', { limit: 100 }, mockLog);
            const bucket2 = tokenBucket.getTokenBucket('bucket-2', { limit: 200 }, mockLog);
            const bucket3 = tokenBucket.getTokenBucket('bucket-3', { limit: 300 }, mockLog);

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
});
