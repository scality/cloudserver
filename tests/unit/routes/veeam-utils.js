const assert = require('assert');
const sinon = require('sinon');
const UtilizationService = require('../../../lib/utilization/instance');
const metadata = require('../../../lib/metadata/wrapper');
const { fetchCapacityMetrics, buildVeeamFileData } = require('../../../lib/routes/veeam/utils');
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

    it('should call back with no error and a default date on 404', done => {
        const error404 = new Error('Not Found');
        error404.response = { status: 404 };
        utilizationStub.callsArgWith(4, error404);

        fetchCapacityMetrics(bucketMd, request, log, 'testMethod', (err, metrics) => {
            assert.ifError(err);
            assert(metrics && metrics.date instanceof Date, 'metrics should have a Date for date');
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
            assert(metrics && metrics.date instanceof Date, 'metrics should have a Date for date');
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

describe('buildVeeamFileData', () => {
    let utilizationStub;
    let metadataStub;
    let log;

    const validPath = '.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/';

    const capacityObjectKey = `${validPath}capacity.xml`;
    const systemObjectKey = `${validPath}system.xml`;

    const bucketMd = {
        _name: 'test-bucket',
        _creationDate: '2024-01-01T00:00:00.000Z',
        _capabilities: {
            VeeamSOSApi: {
                CapacityInfo: {
                    Capacity: 1099511627776,
                    Available: 549755813888,
                    Used: 0,
                    LastModified: '2024-01-01T00:00:00.000Z',
                },
            },
        },
    };

    const bucketMdWithSystem = {
        ...bucketMd,
        _capabilities: {
            VeeamSOSApi: {
                SystemInfo: {
                    ProtocolVersion: '1.0',
                    ModelName: 'ARTESCA',
                    LastModified: '2024-01-01T00:00:00.000Z',
                },
            },
        },
    };

    const createRequest = objectKey => ({
        bucketName: 'test-bucket',
        objectKey,
        headers: { host: 'test-bucket.s3.amazonaws.com' },
    });

    beforeEach(() => {
        log = new DummyRequestLogger();
        log.warn = sinon.stub();
        log.error = sinon.stub();

        metadataStub = sinon.stub(metadata, 'getBucket');
        utilizationStub = sinon.stub(UtilizationService, 'getUtilizationMetrics');
    });

    afterEach(() => {
        sinon.restore();
    });

    it('should return InternalError when metadata.getBucket fails', done => {
        metadataStub.callsArgWith(2, new Error('DB error'));

        buildVeeamFileData(createRequest(capacityObjectKey), bucketMd, log, 'test', (err) => {
            assert(err);
            assert.strictEqual(err.code, 500);
            done();
        });
    });

    it('should return NoSuchKey when capabilities do not include the requested file', done => {
        metadataStub.callsArgWith(2, null, { _capabilities: {} });

        buildVeeamFileData(createRequest(capacityObjectKey), bucketMd, log, 'test', (err) => {
            assert(err);
            assert.strictEqual(err.code, 404);
            done();
        });
    });

    it('should return InternalError when fetchCapacityMetrics fails', done => {
        metadataStub.callsArgWith(2, null, bucketMd);
        const error500 = new Error('Internal Server Error');
        error500.response = { status: 500 };
        utilizationStub.callsArgWith(4, error500);

        buildVeeamFileData(createRequest(capacityObjectKey), bucketMd, log, 'test', (err) => {
            assert(err);
            assert.strictEqual(err.code, 500);
            done();
        });
    });

    it('should build capacity.xml with SUR metrics date and applied Used/Available', done => {
        const metricsDate = new Date('2026-03-26T19:00:08.996Z');
        metadataStub.callsArgWith(2, null, bucketMd);
        utilizationStub.callsArgWith(4, null, { date: metricsDate, bytesTotal: 100 });

        buildVeeamFileData(createRequest(capacityObjectKey), bucketMd, log, 'test', (err, result) => {
            assert.ifError(err);
            assert.strictEqual(result.modified, metricsDate);
            assert(result.xmlContent.includes('CapacityInfo'));
            assert(result.xmlContent.includes('<Used>100</Used>'));
            assert(result.xmlContent.includes('<Available>'));
            assert(Buffer.isBuffer(result.dataBuffer));
            assert.deepStrictEqual(result.dataBuffer, Buffer.from(result.xmlContent));
            assert.strictEqual(result.bucketData, bucketMd);
            done();
        });
    });

    it('should use current date for capacity.xml when UtilizationService returns 404', done => {
        const before = Date.now();
        metadataStub.callsArgWith(2, null, bucketMd);
        const error404 = new Error('Not Found');
        error404.response = { status: 404 };
        utilizationStub.callsArgWith(4, error404);

        buildVeeamFileData(createRequest(capacityObjectKey), bucketMd, log, 'test', (err, result) => {
            assert.ifError(err);
            assert(result.modified instanceof Date);
            assert(result.modified.getTime() >= before);
            assert(result.xmlContent.includes('CapacityInfo'));
            done();
        });
    });

    it('should build system.xml without calling UtilizationService', done => {
        metadataStub.callsArgWith(2, null, bucketMdWithSystem);
        const before = Date.now();

        buildVeeamFileData(createRequest(systemObjectKey), bucketMdWithSystem, log, 'test', (err, result) => {
            assert.ifError(err);
            assert(!utilizationStub.called, 'should not call UtilizationService for system.xml');
            assert(result.modified instanceof Date);
            assert(result.modified.getTime() >= before);
            assert(result.xmlContent.includes('SystemInfo'));
            assert(result.xmlContent.includes('ARTESCA'));
            assert(Buffer.isBuffer(result.dataBuffer));
            done();
        });
    });

    it('should not overwrite Used when it is already set', done => {
        const bucketMdWithUsed = {
            ...bucketMd,
            _capabilities: {
                VeeamSOSApi: {
                    CapacityInfo: {
                        Capacity: 1000,
                        Available: 600,
                        Used: 400,
                        LastModified: '2024-01-01T00:00:00.000Z',
                    },
                },
            },
        };
        metadataStub.callsArgWith(2, null, bucketMdWithUsed);
        utilizationStub.callsArgWith(4, null, { date: new Date(), bytesTotal: 999 });

        buildVeeamFileData(createRequest(capacityObjectKey), bucketMdWithUsed, log, 'test', (err, result) => {
            assert.ifError(err);
            assert(result.xmlContent.includes('<Used>400</Used>'), 'should keep existing Used value');
            done();
        });
    });
});
