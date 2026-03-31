const assert = require('assert');
const sinon = require('sinon');
const UtilizationService = require('../../../lib/utilization/instance');
const { fetchCapacityMetrics } = require('../../../lib/routes/veeam/utils');
const { DummyRequestLogger } = require('../helpers');

describe('fetchCapacityMetrics', () => {
    let utilizationStub;
    let log;
    let logWarnSpy;
    let logErrorSpy;

    const bucketMd = {
        _name: 'test-bucket',
        _creationDate: '2024-01-01T00:00:00.000Z',
    };

    const request = {
        bucketName: 'test-bucket',
    };

    beforeEach(() => {
        log = new DummyRequestLogger();
        logWarnSpy = sinon.spy();
        logErrorSpy = sinon.spy();
        log.warn = logWarnSpy;
        log.error = logErrorSpy;

        utilizationStub = sinon.stub(UtilizationService, 'getUtilizationMetrics');
    });

    afterEach(() => {
        sinon.restore();
    });

    it('should call UtilizationService with the correct bucket key', done => {
        utilizationStub.callsArgWith(4, null, {});

        fetchCapacityMetrics(bucketMd, request, log, 'testMethod', () => {
            const expectedKey = `test-bucket_${new Date('2024-01-01T00:00:00.000Z').getTime()}`;
            assert.strictEqual(utilizationStub.getCall(0).args[0], 'bucket');
            assert.strictEqual(utilizationStub.getCall(0).args[1], expectedKey);
            done();
        });
    });

    it('should call back with metrics on success', done => {
        const bucketMetrics = { bytesTotal: 42, date: '2026-03-26T19:00:08.996Z' };
        utilizationStub.callsArgWith(4, null, bucketMetrics);

        fetchCapacityMetrics(bucketMd, request, log, 'testMethod', (err, metrics) => {
            assert.ifError(err);
            assert.strictEqual(metrics, bucketMetrics);
            assert(!logWarnSpy.called);
            assert(!logErrorSpy.called);
            done();
        });
    });

    it('should call back with no error and no metrics on 404', done => {
        const error404 = new Error('Not Found');
        error404.response = { status: 404 };
        utilizationStub.callsArgWith(4, error404);

        fetchCapacityMetrics(bucketMd, request, log, 'testMethod', (err, metrics) => {
            assert.ifError(err);
            assert.strictEqual(metrics, undefined);
            assert(logWarnSpy.calledOnce);
            assert(logWarnSpy.getCall(0).args[0].includes('404'));
            assert.strictEqual(logWarnSpy.getCall(0).args[1].method, 'testMethod');
            assert.strictEqual(logWarnSpy.getCall(0).args[1].bucket, 'test-bucket');
            assert(!logErrorSpy.called);
            done();
        });
    });

    it('should also handle 404 via statusCode property', done => {
        const error404 = new Error('Not Found');
        error404.statusCode = 404;
        utilizationStub.callsArgWith(4, error404);

        fetchCapacityMetrics(bucketMd, request, log, 'testMethod', (err, metrics) => {
            assert.ifError(err);
            assert.strictEqual(metrics, undefined);
            assert(logWarnSpy.calledOnce);
            done();
        });
    });

    it('should call back with error on non-404 failures', done => {
        const error500 = new Error('Internal Server Error');
        error500.response = { status: 500 };
        utilizationStub.callsArgWith(4, error500);

        fetchCapacityMetrics(bucketMd, request, log, 'testMethod', (err, metrics) => {
            assert.strictEqual(err, error500);
            assert.strictEqual(metrics, undefined);
            assert(logErrorSpy.calledOnce);
            assert.strictEqual(logErrorSpy.getCall(0).args[1].method, 'testMethod');
            assert.strictEqual(logErrorSpy.getCall(0).args[1].bucket, 'test-bucket');
            assert.strictEqual(logErrorSpy.getCall(0).args[1].statusCode, 500);
            assert(!logWarnSpy.called);
            done();
        });
    });

    it('should call back with error on connection errors', done => {
        const connError = new Error('Connection refused');
        connError.code = 'ECONNREFUSED';
        utilizationStub.callsArgWith(4, connError);

        fetchCapacityMetrics(bucketMd, request, log, 'testMethod', (err, metrics) => {
            assert.strictEqual(err, connError);
            assert.strictEqual(metrics, undefined);
            assert(logErrorSpy.calledOnce);
            assert.strictEqual(logErrorSpy.getCall(0).args[1].statusCode, 'ECONNREFUSED');
            done();
        });
    });
});
