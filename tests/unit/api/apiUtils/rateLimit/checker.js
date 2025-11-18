const assert = require('assert');
const sinon = require('sinon');

const cache = require('../../../../../lib/api/apiUtils/rateLimit/cache');
const gcra = require('../../../../../lib/api/apiUtils/rateLimit/gcra');
const metadata = require('../../../../../lib/metadata/wrapper');
const { config } = require('../../../../../lib/Config');
const constants = require('../../../../../constants');

describe('Rate limit checker', () => {
    let mockLog;
    let getCounterStub;
    let setCounterStub;
    let evaluateStub;
    let getBucketStub;
    let getCachedConfigStub;
    let checkRateLimit;

    beforeEach(() => {
        mockLog = {
            trace: sinon.stub(),
            debug: sinon.stub(),
            info: sinon.stub(),
            error: sinon.stub(),
        };
        getCounterStub = sinon.stub(cache, 'getCounter');
        setCounterStub = sinon.stub(cache, 'setCounter');
        getCachedConfigStub = sinon.stub(cache, 'getCachedConfig');
        evaluateStub = sinon.stub(gcra, 'evaluate');
        getBucketStub = sinon.stub(metadata, 'getBucket');

        // Clear module cache and require checker fresh for each test
        delete require.cache[require.resolve(
            '../../../../../lib/api/apiUtils/rateLimit/checker'
        )];
        checkRateLimit = require(
            '../../../../../lib/api/apiUtils/rateLimit/checker'
        ).checkRateLimit;
    });

    afterEach(() => {
        getCounterStub.restore();
        setCounterStub.restore();
        getCachedConfigStub.restore();
        evaluateStub.restore();
        getBucketStub.restore();
    });

    describe('when rate limiting is disabled', () => {
        it('should allow request without checking config', done => {
            const originalEnabled = config.rateLimiting?.enabled;
            if (config.rateLimiting) {
                config.rateLimiting.enabled = false;
            }

            checkRateLimit('test-bucket', mockLog, (err, rateLimited) => {
                assert.ifError(err);
                assert.strictEqual(rateLimited, false);
                assert(getBucketStub.notCalled);

                // Restore
                if (config.rateLimiting) {
                    config.rateLimiting.enabled = originalEnabled;
                }
                done();
            });
        });
    });

    describe('when rate limiting is enabled', () => {
        beforeEach(() => {
            // Ensure rate limiting is enabled for these tests
            if (!config.rateLimiting) {
                config.rateLimiting = {};
            }
            config.rateLimiting.enabled = true;
        });

        it('should allow request when no rate limit is configured', done => {
            // Cache returns null (no limit configured)
            getCachedConfigStub.returns(null);

            checkRateLimit('test-bucket', mockLog, (err, rateLimited) => {
                assert.ifError(err);
                assert.strictEqual(rateLimited, false);
                assert(getCachedConfigStub.calledOnce);
                assert(evaluateStub.notCalled);
                done();
            });
        });

        it('should allow request when limit is 0 (unlimited)', done => {
            // Cache returns limit of 0
            getCachedConfigStub.returns({ limit: 0, source: 'global' });

            checkRateLimit('test-bucket', mockLog, (err, rateLimited) => {
                assert.ifError(err);
                assert.strictEqual(rateLimited, false);
                assert(evaluateStub.notCalled);
                done();
            });
        });

        it('should fail open when config resolution fails', done => {
            // Cache miss
            getCachedConfigStub.returns(undefined);

            // Metadata service error
            const testError = new Error('Metadata service unavailable');
            getBucketStub.callsFake((bucketName, log, cb) => {
                cb(testError);
            });

            checkRateLimit('test-bucket', mockLog, (err, rateLimited) => {
                assert.ifError(err);
                assert.strictEqual(rateLimited, false);
                assert(mockLog.error.calledWith(
                    'Failed to resolve rate limit config, failing open'
                ));
                done();
            });
        });

        it('should evaluate GCRA and allow request if within limit', done => {
            // Cache returns limit config
            getCachedConfigStub.returns({ limit: 100, source: 'bucket' });

            getCounterStub.returns(1000);
            evaluateStub.returns({
                allowed: true,
                newEmptyAt: 2000,
            });

            checkRateLimit('test-bucket', mockLog, (err, rateLimited) => {
                assert.ifError(err);
                assert.strictEqual(rateLimited, false);
                assert(evaluateStub.calledOnce);
                assert(setCounterStub.calledOnceWith(
                    'bucket:test-bucket:rps',
                    2000
                ));
                assert(mockLog.debug.calledWith('Rate limit check completed'));
                done();
            });
        });

        it('should evaluate GCRA and deny request if over limit', done => {
            // Cache returns limit config
            getCachedConfigStub.returns({ limit: 100, source: 'bucket' });

            getCounterStub.returns(1000);
            evaluateStub.returns({
                allowed: false,
                newEmptyAt: 2000,
            });

            checkRateLimit('test-bucket', mockLog, (err, rateLimited) => {
                assert.ifError(err);
                assert.strictEqual(rateLimited, true);
                assert(evaluateStub.calledOnce);
                assert(setCounterStub.notCalled);
                assert(mockLog.info.calledWith('Rate limit check completed'));
                done();
            });
        });

        it('should use correct counter key for bucket', done => {
            // Cache returns limit config
            getCachedConfigStub.returns({ limit: 100, source: 'bucket' });

            getCounterStub.returns(undefined);
            evaluateStub.returns({
                allowed: true,
                newEmptyAt: 2000,
            });

            checkRateLimit('my-test-bucket', mockLog, err => {
                assert.ifError(err);
                assert(getCounterStub.calledOnceWith('bucket:my-test-bucket:rps'));
                assert(setCounterStub.calledWith('bucket:my-test-bucket:rps'));
                done();
            });
        });

        it('should use 0 as emptyAt when counter does not exist', done => {
            // Cache returns limit config
            getCachedConfigStub.returns({ limit: 100, source: 'bucket' });

            getCounterStub.returns(undefined);
            evaluateStub.returns({
                allowed: true,
                newEmptyAt: 1000,
            });

            checkRateLimit('test-bucket', mockLog, err => {
                assert.ifError(err);
                const evaluateCall = evaluateStub.firstCall.args;
                assert.strictEqual(evaluateCall[0], 0); // emptyAt should be 0
                done();
            });
        });

        it('should calculate interval using distributed architecture', done => {
            // Cache returns limit config
            getCachedConfigStub.returns({ limit: 100, source: 'bucket' });

            getCounterStub.returns(0);
            evaluateStub.returns({
                allowed: true,
                newEmptyAt: 1000,
            });

            // Mock config values
            const originalNodes = config.rateLimiting.nodes;
            const originalClusters = config.clusters;
            config.rateLimiting.nodes = 2;
            config.clusters = 4;

            checkRateLimit('test-bucket', mockLog, err => {
                assert.ifError(err);

                const evaluateCall = evaluateStub.firstCall.args;
                const interval = evaluateCall[2];

                // With limit=100, nodes=2, workers=4:
                // interval = 1000 / (100 / 2 / 4) = 1000 / 12.5 = 80ms
                assert.strictEqual(interval, 80);

                // Restore
                config.rateLimiting.nodes = originalNodes;
                config.clusters = originalClusters;
                done();
            });
        });

        it('should use default burst capacity from constants', done => {
            // Cache returns limit config
            getCachedConfigStub.returns({ limit: 100, source: 'bucket' });

            getCounterStub.returns(0);
            evaluateStub.returns({
                allowed: true,
                newEmptyAt: 1000,
            });

            // Ensure no burst capacity in config
            const originalBucket = config.rateLimiting.bucket;
            config.rateLimiting.bucket = undefined;

            checkRateLimit('test-bucket', mockLog, err => {
                assert.ifError(err);

                const evaluateCall = evaluateStub.firstCall.args;
                const bucketSize = evaluateCall[3];

                // Should use default from constants
                const expectedBucketSize =
                    constants.rateLimitDefaultBurstCapacity * 1000;
                assert.strictEqual(bucketSize, expectedBucketSize);

                // Restore
                config.rateLimiting.bucket = originalBucket;
                done();
            });
        });

        it('should use configured burst capacity if available', done => {
            // Cache returns limit config
            getCachedConfigStub.returns({ limit: 100, source: 'bucket' });

            getCounterStub.returns(0);
            evaluateStub.returns({
                allowed: true,
                newEmptyAt: 1000,
            });

            // Set custom burst capacity
            const originalBucket = config.rateLimiting.bucket;
            config.rateLimiting.bucket = {
                defaultBurstCapacity: 5,
            };

            checkRateLimit('test-bucket', mockLog, err => {
                assert.ifError(err);

                const evaluateCall = evaluateStub.firstCall.args;
                const bucketSize = evaluateCall[3];

                // Should use configured burst capacity
                assert.strictEqual(bucketSize, 5000);

                // Restore
                config.rateLimiting.bucket = originalBucket;
                done();
            });
        });

        it('should log detailed information on allowed request', done => {
            // Cache returns limit config
            getCachedConfigStub.returns({ limit: 100, source: 'global' });

            getCounterStub.returns(1000);
            evaluateStub.returns({
                allowed: true,
                newEmptyAt: 2000,
            });

            checkRateLimit('test-bucket', mockLog, err => {
                assert.ifError(err);
                assert(mockLog.debug.calledWith('Rate limit check completed'));

                const logCall = mockLog.debug.firstCall.args[1];
                assert.strictEqual(logCall.bucketName, 'test-bucket');
                assert.strictEqual(logCall.rateLimited, false);
                assert.strictEqual(logCall.rateLimitSource, 'global');
                assert.strictEqual(logCall.decision, 'allowed');
                assert.strictEqual(logCall.limit, 100);
                assert.strictEqual(logCall.emptyAt, 1000);
                assert.strictEqual(logCall.newEmptyAt, 2000);
                done();
            });
        });

        it('should log detailed information on denied request', done => {
            // Cache returns limit config
            getCachedConfigStub.returns({ limit: 100, source: 'bucket' });

            getCounterStub.returns(1000);
            evaluateStub.returns({
                allowed: false,
                newEmptyAt: 2000,
            });

            checkRateLimit('test-bucket', mockLog, err => {
                assert.ifError(err);
                assert(mockLog.info.calledWith('Rate limit check completed'));

                const logCall = mockLog.info.firstCall.args[1];
                assert.strictEqual(logCall.bucketName, 'test-bucket');
                assert.strictEqual(logCall.rateLimited, true);
                assert.strictEqual(logCall.rateLimitSource, 'bucket');
                assert.strictEqual(logCall.decision, 'denied');
                done();
            });
        });
    });
});
