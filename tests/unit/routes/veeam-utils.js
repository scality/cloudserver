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

    it('should call UtilizationService with the correct bucket key', async () => {
        utilizationStub.callsArgWith(5, null, {});

        await fetchCapacityMetrics(bucketMd, request, log);

        const expectedKey = `test-bucket_${new Date('2024-01-01T00:00:00.000Z').getTime()}`;
        assert.strictEqual(utilizationStub.getCall(0).args[0], 'bucket');
        assert.strictEqual(utilizationStub.getCall(0).args[1], expectedKey);
    });

    it('should resolve with metrics on success', async () => {
        const bucketMetrics = { bytesTotal: 42, date: '2026-03-26T19:00:08.996Z' };
        utilizationStub.callsArgWith(5, null, bucketMetrics);

        const metrics = await fetchCapacityMetrics(bucketMd, request, log);

        assert.strictEqual(metrics, bucketMetrics);
        assert(!logWarnSpy.called);
        assert(!logErrorSpy.called);
    });

    it('should resolve with no error and a default date on 404', async () => {
        const error404 = new Error('Not Found');
        error404.response = { status: 404 };
        utilizationStub.callsArgWith(5, error404);

        const metrics = await fetchCapacityMetrics(bucketMd, request, log);

        assert(metrics && metrics.date instanceof Date, 'metrics should have a Date for date');
        assert(logWarnSpy.calledOnce);
        assert(logWarnSpy.getCall(0).args[0].includes('404'));
        assert.strictEqual(logWarnSpy.getCall(0).args[1].bucket, 'test-bucket');
        assert(!logErrorSpy.called);
    });

    it('should also handle 404 via statusCode property', async () => {
        const error404 = new Error('Not Found');
        error404.statusCode = 404;
        utilizationStub.callsArgWith(5, error404);

        const metrics = await fetchCapacityMetrics(bucketMd, request, log);

        assert(metrics && metrics.date instanceof Date, 'metrics should have a Date for date');
        assert(logWarnSpy.calledOnce);
    });

    it('should reject with error on non-404 failures', async () => {
        const error500 = new Error('Internal Server Error');
        error500.response = { status: 500 };
        utilizationStub.callsArgWith(5, error500);

        await assert.rejects(fetchCapacityMetrics(bucketMd, request, log), err => err === error500);

        assert(logErrorSpy.calledOnce);
        assert.strictEqual(logErrorSpy.getCall(0).args[1].bucket, 'test-bucket');
        assert.strictEqual(logErrorSpy.getCall(0).args[1].statusCode, 500);
        assert(!logWarnSpy.called);
    });

    it('should reject with error on connection errors', async () => {
        const connError = new Error('Connection refused');
        connError.code = 'ECONNREFUSED';
        utilizationStub.callsArgWith(5, connError);

        await assert.rejects(fetchCapacityMetrics(bucketMd, request, log), err => err === connError);

        assert(logErrorSpy.calledOnce);
        assert.strictEqual(logErrorSpy.getCall(0).args[1].statusCode, 'ECONNREFUSED');
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

    it('should reject with InternalError when metadata.getBucket fails', async () => {
        metadataStub.callsArgWith(2, new Error('DB error'));

        await assert.rejects(
            buildVeeamFileData(createRequest(capacityObjectKey), bucketMd, log),
            err => err.code === 500,
        );
    });

    it('should reject with NoSuchKey when capabilities do not include the requested file', async () => {
        metadataStub.callsArgWith(2, null, { _capabilities: {} });

        await assert.rejects(
            buildVeeamFileData(createRequest(capacityObjectKey), bucketMd, log),
            err => err.code === 404,
        );
    });

    it('should reject with InternalError when fetchCapacityMetrics fails', async () => {
        metadataStub.callsArgWith(2, null, bucketMd);
        const error500 = new Error('Internal Server Error');
        error500.response = { status: 500 };
        utilizationStub.callsArgWith(5, error500);

        await assert.rejects(
            buildVeeamFileData(createRequest(capacityObjectKey), bucketMd, log),
            err => err.code === 500,
        );
    });

    it('should build capacity.xml with SUR metrics date and applied Used/Available', async () => {
        const metricsDate = new Date('2026-03-26T19:00:08.996Z');
        metadataStub.callsArgWith(2, null, bucketMd);
        utilizationStub.callsArgWith(5, null, { date: metricsDate, bytesTotal: 100 });

        const result = await buildVeeamFileData(createRequest(capacityObjectKey), bucketMd, log);

        assert.strictEqual(result.modified, metricsDate);
        assert(result.xmlContent.includes('CapacityInfo'));
        assert(result.xmlContent.includes('<Used>100</Used>'));
        assert(result.xmlContent.includes('<Available>'));
        assert(Buffer.isBuffer(result.dataBuffer));
        assert.deepStrictEqual(result.dataBuffer, Buffer.from(result.xmlContent));
        assert.strictEqual(result.bucketData, bucketMd);
    });

    it('should use current date for capacity.xml when UtilizationService returns 404', async () => {
        const before = Date.now();
        metadataStub.callsArgWith(2, null, bucketMd);
        const error404 = new Error('Not Found');
        error404.response = { status: 404 };
        utilizationStub.callsArgWith(5, error404);

        const result = await buildVeeamFileData(createRequest(capacityObjectKey), bucketMd, log);

        assert(result.modified instanceof Date);
        assert(result.modified.getTime() >= before);
        assert(result.xmlContent.includes('CapacityInfo'));
    });

    it('should build system.xml without calling UtilizationService', async () => {
        metadataStub.callsArgWith(2, null, bucketMdWithSystem);
        const before = Date.now();

        const result = await buildVeeamFileData(createRequest(systemObjectKey), bucketMdWithSystem, log);

        assert(!utilizationStub.called, 'should not call UtilizationService for system.xml');
        assert(result.modified instanceof Date);
        assert(result.modified.getTime() >= before);
        assert(result.xmlContent.includes('SystemInfo'));
        assert(result.xmlContent.includes('ARTESCA'));
        assert(Buffer.isBuffer(result.dataBuffer));
    });

    it('should not overwrite Used when it is already set', async () => {
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
        utilizationStub.callsArgWith(5, null, { date: new Date(), bytesTotal: 999 });

        const result = await buildVeeamFileData(createRequest(capacityObjectKey), bucketMdWithUsed, log);

        assert(result.xmlContent.includes('<Used>400</Used>'), 'should keep existing Used value');
    });
});
