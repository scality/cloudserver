const assert = require('assert');
const sinon = require('sinon');

const {
    startCleanupJob,
    stopCleanupJob,
} = require('../../../../../lib/api/apiUtils/rateLimit/cleanup');
const constants = require('../../../../../constants');

describe('Rate limit cleanup job', () => {
    let mockLog;
    let setIntervalSpy;
    let clearIntervalSpy;

    beforeEach(() => {
        mockLog = {
            info: sinon.stub(),
            warn: sinon.stub(),
            debug: sinon.stub(),
        };
        setIntervalSpy = sinon.spy(global, 'setInterval');
        clearIntervalSpy = sinon.spy(global, 'clearInterval');
    });

    afterEach(() => {
        stopCleanupJob();
        setIntervalSpy.restore();
        clearIntervalSpy.restore();
    });

    it('should start cleanup job successfully', () => {
        startCleanupJob(mockLog, { skipUnref: true });

        assert(mockLog.info.calledOnce);
        assert(mockLog.info.calledWith('Starting rate limit cleanup job', {
            interval: constants.rateLimitCleanupInterval,
        }));
        assert(setIntervalSpy.calledOnce);
        assert.strictEqual(setIntervalSpy.firstCall.args[1], constants.rateLimitCleanupInterval);
    });

    it('should not start cleanup job if already running', () => {
        startCleanupJob(mockLog, { skipUnref: true });
        mockLog.info.resetHistory();
        mockLog.warn.resetHistory();
        setIntervalSpy.resetHistory();

        startCleanupJob(mockLog, { skipUnref: true });

        assert(mockLog.warn.calledOnce);
        assert(mockLog.warn.calledWith('Rate limit cleanup job already running'));
        assert(mockLog.info.notCalled);
        assert(setIntervalSpy.notCalled);
    });

    it('should stop cleanup job successfully', () => {
        startCleanupJob(mockLog, { skipUnref: true });
        mockLog.info.resetHistory();

        stopCleanupJob(mockLog);

        assert(mockLog.info.calledOnce);
        assert(mockLog.info.calledWith('Stopped rate limit cleanup job'));
        assert(clearIntervalSpy.calledOnce);
    });

    it('should not error when stopping cleanup job that is not running', () => {
        assert.doesNotThrow(() => {
            stopCleanupJob(mockLog);
        });
        assert(clearIntervalSpy.notCalled);
    });

    it('should allow restarting cleanup job after stopping', () => {
        startCleanupJob(mockLog, { skipUnref: true });
        assert(setIntervalSpy.calledOnce);

        stopCleanupJob(mockLog);
        assert(clearIntervalSpy.calledOnce);

        setIntervalSpy.resetHistory();
        clearIntervalSpy.resetHistory();
        mockLog.info.resetHistory();

        startCleanupJob(mockLog, { skipUnref: true });
        assert(setIntervalSpy.calledOnce);
        assert(mockLog.info.calledOnce);
    });

    it('should call unref() on interval by default', () => {
        const mockUnref = sinon.stub();
        setIntervalSpy.restore();
        setIntervalSpy = sinon.stub(global, 'setInterval').returns({
            unref: mockUnref,
        });

        startCleanupJob(mockLog);

        assert(mockUnref.calledOnce);

        stopCleanupJob(mockLog);
        setIntervalSpy.restore();
    });

    it('should not call unref() when skipUnref is true', () => {
        const mockUnref = sinon.stub();
        setIntervalSpy.restore();
        setIntervalSpy = sinon.stub(global, 'setInterval').returns({
            unref: mockUnref,
        });

        startCleanupJob(mockLog, { skipUnref: true });

        assert(mockUnref.notCalled);

        stopCleanupJob(mockLog);
        setIntervalSpy.restore();
    });
});
