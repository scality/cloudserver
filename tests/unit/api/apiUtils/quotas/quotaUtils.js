const sinon = require('sinon');
const assert = require('assert');
const { config } = require('../../../../../lib/Config');
const {
    validateQuotas,
    processBytesToWrite,
    isMetricStale,
} = require('../../../../../lib/api/apiUtils/quotas/quotaUtils');
const QuotaService = require('../../../../../lib/quotas/quotas');

const mockLog = {
    warn: sinon.stub(),
    debug: sinon.stub(),
};

const mockBucket = {
    getQuota: () => 100,
    getName: () => 'bucketName',
    getCreationDate: () => '2022-01-01T00:00:00.000Z',
};

const mockBucketNoQuota = {
    getQuota: () => 0,
    getName: () => 'bucketName',
    getCreationDate: () => '2022-01-01T00:00:00.000Z',
};

describe('validateQuotas (buckets)', () => {
    const request = {
        getQuota: () => 100,
    };

    beforeEach(() => {
        config.scuba = {
            host: 'localhost',
            port: 8080,
        };
        config.quota = {
            maxStaleness: 24 * 60 * 60 * 1000,
            enableInflights: true,
        };
        config.isQuotaEnabled = sinon.stub().returns(true);
        QuotaService.enabled = true;
        QuotaService._getLatestMetricsCallback = sinon.stub().resolves({});
        request.finalizerHooks = [];
    });

    afterEach(() => {
        sinon.restore();
    });

    it('should return null if quota is <= 0', done => {
        validateQuotas(request, mockBucketNoQuota, {}, [], '', false, mockLog, err => {
            assert.ifError(err);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.called, false);
            done();
        });
    });

    it('should return null if scuba is disabled', done => {
        QuotaService.enabled = false;
        validateQuotas(request, mockBucket, {}, [], '', false, mockLog, err => {
            assert.ifError(err);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.called, false);
            done();
        });
    });

    it('should return null if metrics retrieval fails', done => {
        QuotaService.enabled = true;
        const error = new Error('Failed to get metrics');
        QuotaService._getLatestMetricsCallback.yields(error);

        validateQuotas(request, mockBucket, {}, ['objectPut', 'getObject'], 'objectPut', 1, mockLog, err => {
            assert.ifError(err);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.calledOnce, true);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.calledWith(
                'bucket',
                'bucketName_1640995200000',
                null,
                {
                    action: 'objectPut',
                    inflight: 1,
                }
            ), true);
            done();
        });
    });

    it('should return errors.QuotaExceeded if quota is exceeded', done => {
        const result1 = {
            bytesTotal: 150,
        };
        const result2 = {
            bytesTotal: 120,
        };
        QuotaService._getLatestMetricsCallback.yields(null, result1);
        QuotaService._getLatestMetricsCallback.yields(null, result2);

        validateQuotas(request, mockBucket, {}, ['objectPut', 'getObject'], 'objectPut', 1, mockLog, err => {
            assert.strictEqual(err.is.QuotaExceeded, true);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.callCount, 1);
            assert.strictEqual(request.finalizerHooks.length, 1);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.calledWith(
                'bucket',
                'bucketName_1640995200000',
                null,
                {
                    action: 'objectPut',
                    inflight: 1,
                }
            ), true);
            done();
        });
    });

    it('should not return QuotaExceeded if the quotas are exceeded but operation is a delete', done => {
        const result1 = {
            bytesTotal: 150,
        };
        const result2 = {
            bytesTotal: 120,
        };
        QuotaService._getLatestMetricsCallback.yields(null, result1);
        QuotaService._getLatestMetricsCallback.onCall(1).yields(null, result2);

        validateQuotas(request, mockBucket, {}, ['objectDelete'], 'objectDelete', -50, mockLog, err => {
            assert.ifError(err);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.calledOnce, true);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.calledWith(
                'bucket',
                'bucketName_1640995200000',
                null,
                {
                    action: 'objectDelete',
                    inflight: -50,
                }
            ), true);
            done();
        });
    });

    it('should return null if quota is not exceeded', done => {
        const result1 = {
            bytesTotal: 80,
        };
        const result2 = {
            bytesTotal: 90,
        };
        QuotaService._getLatestMetricsCallback.yields(null, result1);
        QuotaService._getLatestMetricsCallback.onCall(1).yields(null, result2);

        validateQuotas(request, mockBucket, {}, ['objectRestore', 'objectPut'], 'objectRestore',
            true, mockLog, err => {
                assert.ifError(err);
                assert.strictEqual(QuotaService._getLatestMetricsCallback.calledTwice, true);
                assert.strictEqual(QuotaService._getLatestMetricsCallback.calledWith(
                    'bucket',
                    'bucketName_1640995200000',
                    null,
                    {
                        action: 'objectRestore',
                        inflight: true,
                    }
                ), true);
                done();
            });
    });

    it('should not include the inflights in the request if they are disabled', done => {
        config.quota.enableInflights = false;
        const result1 = {
            bytesTotal: 80,
        };
        const result2 = {
            bytesTotal: 90,
        };
        QuotaService._getLatestMetricsCallback.yields(null, result1);
        QuotaService._getLatestMetricsCallback.onCall(1).yields(null, result2);

        validateQuotas(request, mockBucket, {}, ['objectRestore', 'objectPut'], 'objectRestore',
            true, mockLog, err => {
                assert.ifError(err);
                assert.strictEqual(QuotaService._getLatestMetricsCallback.calledTwice, true);
                assert.strictEqual(QuotaService._getLatestMetricsCallback.calledWith(
                    'bucket',
                    'bucketName_1640995200000',
                    null,
                    {
                        action: 'objectRestore',
                    }
                ), true);
            done();
        });
    });
});

describe('validateQuotas (with accounts)', () => {
    const request = {
        getQuota: () => 100,
    };

    beforeEach(() => {
        config.scuba = {
            host: 'localhost',
            port: 8080,
        };
        config.quota = {
            maxStaleness: 24 * 60 * 60 * 1000,
            enableInflights: true,
        };
        request.finalizerHooks = [];
        config.isQuotaEnabled = sinon.stub().returns(true);
        QuotaService.enabled = true;
        QuotaService._getLatestMetricsCallback = sinon.stub().resolves({});
    });

    afterEach(() => {
        sinon.restore();
    });

    it('should return null if quota is <= 0', done => {
        validateQuotas(request, mockBucketNoQuota, {
            account: 'test_1',
            quota: 0,
        }, [], '', false, mockLog, err => {
            assert.ifError(err);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.called, false);
            done();
        });
    });

    it('should not return null if bucket quota is <= 0 but account quota is > 0', done => {
        validateQuotas(request, mockBucketNoQuota, {
            account: 'test_1',
            quota: 1000,
        }, [], '', false, mockLog, err => {
            assert.ifError(err);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.called, false);
            done();
        });
    });

    it('should return null if scuba is disabled', done => {
        QuotaService.enabled = false;
        validateQuotas(request, mockBucket, {
            account: 'test_1',
            quota: 1000,
        }, [], '', false, mockLog, err => {
            assert.ifError(err);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.called, false);
            done();
        });
    });

    it('should return null if metrics retrieval fails', done => {
        QuotaService.enabled = true;
        const error = new Error('Failed to get metrics');
        QuotaService._getLatestMetricsCallback.yields(error);

        validateQuotas(request, mockBucket, {
            account: 'test_1',
            quota: 1000,
        }, ['objectPut', 'getObject'], 'objectPut', 1, mockLog, err => {
            assert.ifError(err);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.calledOnce, true);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.calledWith(
                'bucket',
                'bucketName_1640995200000',
                null,
                {
                    action: 'objectPut',
                    inflight: 1,
                }
            ), true);
            done();
        });
    });

    it('should return errors.QuotaExceeded if quota is exceeded', done => {
        const result1 = {
            bytesTotal: 150,
        };
        const result2 = {
            bytesTotal: 120,
        };
        QuotaService._getLatestMetricsCallback.yields(null, result1);
        QuotaService._getLatestMetricsCallback.onCall(1).yields(null, result2);

        validateQuotas(request, mockBucketNoQuota, {
            account: 'test_1',
            quota: 100,
        }, ['objectPut', 'getObject'], 'objectPut', 1, mockLog, err => {
            assert.strictEqual(err.is.QuotaExceeded, true);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.callCount, 1);
            assert.strictEqual(request.finalizerHooks.length, 1);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.calledWith(
                'account',
                'test_1',
                null,
                {
                    action: 'objectPut',
                    inflight: 1,
                }
            ), true);
            done();
        });
    });

    it('should not return QuotaExceeded if the quotas are exceeded but operation is a delete', done => {
        const result1 = {
            bytesTotal: 150,
        };
        const result2 = {
            bytesTotal: 120,
        };
        QuotaService._getLatestMetricsCallback.yields(null, result1);
        QuotaService._getLatestMetricsCallback.onCall(1).yields(null, result2);

        validateQuotas(request, mockBucketNoQuota, {
            account: 'test_1',
            quota: 1000,
        }, ['objectDelete'], 'objectDelete', -50, mockLog, err => {
            assert.ifError(err);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.callCount, 1);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.calledWith(
                'account',
                'test_1',
                null,
                {
                    action: 'objectDelete',
                    inflight: -50,
                }
            ), true);
            done();
        });
    });

    it('should return null if quota is not exceeded', done => {
        const result1 = {
            bytesTotal: 80,
        };
        const result2 = {
            bytesTotal: 90,
        };
        QuotaService._getLatestMetricsCallback.yields(null, result1);
        QuotaService._getLatestMetricsCallback.onCall(1).yields(null, result2);

        validateQuotas(request, mockBucket, {
            account: 'test_1',
            quota: 1000,
        }, ['objectRestore', 'objectPut'], 'objectRestore', true, mockLog, err => {
            assert.ifError(err);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.callCount, 4);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.calledWith(
                'account',
                'test_1',
                null,
                {
                    action: 'objectRestore',
                    inflight: true,
                }
            ), true);
            done();
        });
    });

    it('should return quota exceeded if account and bucket quotas are different', done => {
        const result1 = {
            bytesTotal: 150,
        };
        const result2 = {
            bytesTotal: 120,
        };
        QuotaService._getLatestMetricsCallback.yields(null, result1);
        QuotaService._getLatestMetricsCallback.onCall(1).yields(null, result2);

        validateQuotas(request, mockBucket, {
            account: 'test_1',
            quota: 1000,
        }, ['objectPut', 'getObject'], 'objectPut', 1, mockLog, err => {
            assert.strictEqual(err.is.QuotaExceeded, true);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.callCount, 2);
            assert.strictEqual(request.finalizerHooks.length, 1);
            done();
        });
    });

    it('should update the request with one function per action to clear quota updates', done => {
        const result1 = {
            bytesTotal: 80,
        };
        const result2 = {
            bytesTotal: 90,
        };
        QuotaService._getLatestMetricsCallback.yields(null, result1);
        QuotaService._getLatestMetricsCallback.onCall(1).yields(null, result2);

        validateQuotas(request, mockBucket, {
            account: 'test_1',
            quota: 1000,
        }, ['objectRestore', 'objectPut'], 'objectRestore', true, mockLog, err => {
            assert.ifError(err);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.callCount, 4);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.calledWith(
                'account',
                'test_1',
                null,
                {
                    action: 'objectRestore',
                    inflight: true,
                }
            ), true);
            done();
        });
    });
});

describe('processBytesToWrite', () => {
    let bucket;
    let versionId;
    let contentLength;
    let objMD;

    beforeEach(() => {
        bucket = {
            isVersioningEnabled: sinon.stub(),
        };
        versionId = '';
        contentLength = 0;
        objMD = null;
    });

    it('should return a negative number if the operation is a delete and bucket is not versioned', () => {
        bucket.isVersioningEnabled.returns(false);
        objMD = { 'content-length': 100 };

        const bytes = processBytesToWrite('objectDelete', bucket, versionId, contentLength, objMD);

        assert.strictEqual(bytes, -100);
    });

    it('should return 0 if the operation is a delete and bucket is versioned', () => {
        bucket.isVersioningEnabled.returns(true);
        objMD = { 'content-length': 100 };

        const bytes = processBytesToWrite('objectDelete', bucket, versionId, contentLength, objMD);

        assert.strictEqual(bytes, 0);
    });

    it('should return a negative number for a versioned bucket with a versionid deletion', () => {
        bucket.isVersioningEnabled.returns(true);
        objMD = { 'content-length': 100 };
        versionId = 'versionId';

        const bytes = processBytesToWrite('objectDelete', bucket, versionId, contentLength, objMD);

        assert.strictEqual(bytes, -100);
    });

    it('should return 0 for a delete operation if the object metadata is missing', () => {
        bucket.isVersioningEnabled.returns(true);
        objMD = null;

        const bytes = processBytesToWrite('objectDelete', bucket, versionId, contentLength, objMD);

        assert.strictEqual(bytes, 0);
    });

    it('should return the object metadata content length for a restore object operation', () => {
        bucket.isVersioningEnabled.returns(true);
        objMD = { 'content-length': 100 };
        contentLength = 150;

        const bytes = processBytesToWrite('objectRestore', bucket, versionId, contentLength, objMD);

        assert.strictEqual(bytes, 100);
    });

    it('should return the difference of the content length if the object is being replaced', () => {
        bucket.isVersioningEnabled.returns(false);
        objMD = { 'content-length': 100 };
        contentLength = 150;

        const bytes = processBytesToWrite('objectPut', bucket, versionId, contentLength, objMD);

        assert.strictEqual(bytes, 50);
    });

    it('should return content length if the object is being replaced and the bucket is versioned', () => {
        bucket.isVersioningEnabled.returns(true);
        objMD = { 'content-length': 100 };
        contentLength = 150;

        const bytes = processBytesToWrite('objectPut', bucket, versionId, contentLength, objMD);

        assert.strictEqual(bytes, contentLength);
    });

    it('should return content length of the object metadata if the action is a copy (getObject authz)', () => {
        bucket.isVersioningEnabled.returns(true);
        objMD = { 'content-length': 100 };

        const bytes = processBytesToWrite('objectCopy', bucket, versionId, 0, objMD);

        assert.strictEqual(bytes, 100);
    });

    it('should return content length of the object metadata if the action is a copy part (getObject authz)', () => {
        bucket.isVersioningEnabled.returns(true);
        objMD = { 'content-length': 100 };

        const bytes = processBytesToWrite('objectPutCopyPart', bucket, versionId, 0, objMD);

        assert.strictEqual(bytes, 100);
    });

    it('should detect object replacement during copy object operation on a non versioned bucket', () => {
        bucket.isVersioningEnabled.returns(false);
        objMD = { 'content-length': 100 };
        const destObjMD = { 'content-length': 20 };

        const bytes = processBytesToWrite('objectCopy', bucket, versionId, 0, objMD, destObjMD);

        assert.strictEqual(bytes, 80);
    });

    it('should not detect object replacement during copy object operation if the bucket is versioned', () => {
        bucket.isVersioningEnabled.returns(true);
        objMD = { 'content-length': 100 };
        const destObjMD = { 'content-length': 20 };

        const bytes = processBytesToWrite('objectCopy', bucket, versionId, 0, objMD, destObjMD);

        assert.strictEqual(bytes, 100);
    });
});

describe('isMetricStale', () => {
    const metric = {
        date: new Date().toISOString(),
    };
    const resourceType = 'bucket';
    const resourceName = 'bucketName';
    const action = 'objectPut';
    const inflight = 1;
    const log = {
        warn: sinon.stub(),
    };

    it('should return false if the metric is not stale', () => {
        const result = isMetricStale(metric, resourceType, resourceName, action, inflight, log);
        assert.strictEqual(result, false);
        assert.strictEqual(log.warn.called, false);
    });

    it('should return true and log a warning if the metric is stale', () => {
        const staleDate = new Date(Date.now() - 24 * 60 * 60 * 1000 - 1);
        metric.date = staleDate.toISOString();

        const result = isMetricStale(metric, resourceType, resourceName, action, inflight, log);
        assert.strictEqual(result, true);
        assert.strictEqual(log.warn.calledOnce, true);
    });
});
