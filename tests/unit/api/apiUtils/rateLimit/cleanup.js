const assert = require('assert');
const sinon = require('sinon');

const {
    startCleanupJob,
    stopCleanupJob,
} = require('../../../../../lib/api/apiUtils/rateLimit/cleanup');
const constants = require('../../../../../constants');

describe('Rate limit cleanup job', () => {
    let mockLog;
    let setTimeoutSpy;
    let clearTimeoutSpy;

    beforeEach(() => {
        mockLog = {
            info: sinon.stub(),
            warn: sinon.stub(),
            debug: sinon.stub(),
        };
        setTimeoutSpy = sinon.spy(global, 'setTimeout');
        clearTimeoutSpy = sinon.spy(global, 'clearTimeout');
    });

    afterEach(() => {
        stopCleanupJob();
        setTimeoutSpy.restore();
        clearTimeoutSpy.restore();
    });

    it('should start cleanup job successfully', () => {
        startCleanupJob(mockLog, { skipUnref: true });

        assert(mockLog.info.calledOnce);
        assert(mockLog.info.calledWith('Starting rate limit cleanup job', {
            interval: constants.rateLimitCleanupInterval,
        }));
        assert(setTimeoutSpy.calledOnce);
        assert.strictEqual(setTimeoutSpy.firstCall.args[1], constants.rateLimitCleanupInterval);
    });

    it('should not start cleanup job if already running', () => {
        startCleanupJob(mockLog, { skipUnref: true });
        mockLog.info.resetHistory();
        mockLog.warn.resetHistory();
        setTimeoutSpy.resetHistory();

        startCleanupJob(mockLog, { skipUnref: true });

        assert(mockLog.warn.calledOnce);
        assert(mockLog.warn.calledWith('Rate limit cleanup job already running'));
        assert(mockLog.info.notCalled);
        assert(setTimeoutSpy.notCalled);
    });

    it('should stop cleanup job successfully', () => {
        startCleanupJob(mockLog, { skipUnref: true });
        mockLog.info.resetHistory();

        stopCleanupJob(mockLog);

        assert(mockLog.info.calledOnce);
        assert(mockLog.info.calledWith('Stopped rate limit cleanup job'));
        assert(clearTimeoutSpy.calledOnce);
    });

    it('should not error when stopping cleanup job that is not running', () => {
        assert.doesNotThrow(() => {
            stopCleanupJob(mockLog);
        });
        assert(clearTimeoutSpy.notCalled);
    });

    it('should allow restarting cleanup job after stopping', () => {
        startCleanupJob(mockLog, { skipUnref: true });
        assert(setTimeoutSpy.calledOnce);

        stopCleanupJob(mockLog);
        assert(clearTimeoutSpy.calledOnce);

        setTimeoutSpy.resetHistory();
        clearTimeoutSpy.resetHistory();
        mockLog.info.resetHistory();

        startCleanupJob(mockLog, { skipUnref: true });
        assert(setTimeoutSpy.calledOnce);
        assert(mockLog.info.calledOnce);
    });

    it('should call unref() on interval by default', () => {
        const mockUnref = sinon.stub();
        setTimeoutSpy.restore();
        setTimeoutSpy = sinon.stub(global, 'setTimeout').returns({
            unref: mockUnref,
        });

        startCleanupJob(mockLog);

        assert(mockUnref.calledOnce);

        stopCleanupJob(mockLog);
        setTimeoutSpy.restore();
    });

    it('should not call unref() when skipUnref is true', () => {
        const mockUnref = sinon.stub();
        setTimeoutSpy.restore();
        setTimeoutSpy = sinon.stub(global, 'setTimeout').returns({
            unref: mockUnref,
        });

        startCleanupJob(mockLog, { skipUnref: true });

        assert(mockUnref.notCalled);

        stopCleanupJob(mockLog);
        setTimeoutSpy.restore();
    });
});
