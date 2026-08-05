const assert = require('assert');
const crypto = require('crypto');
const sinon = require('sinon');
const { Readable } = require('stream');
const UtilizationService = require('../../../lib/utilization/instance');
const metadata = require('../../../lib/metadata/wrapper');
const { fetchCapacityMetrics, buildVeeamFileData, receiveData } = require('../../../lib/routes/veeam/utils');
const { config } = require('../../../lib/Config');
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
        utilizationStub.resolves({});

        await fetchCapacityMetrics(bucketMd, request, log);

        const expectedKey = `test-bucket_${new Date('2024-01-01T00:00:00.000Z').getTime()}`;
        assert.strictEqual(utilizationStub.getCall(0).args[0], 'bucket');
        assert.strictEqual(utilizationStub.getCall(0).args[1], expectedKey);
    });

    it('should resolve with metrics on success', async () => {
        const bucketMetrics = { bytesTotal: 42, date: '2026-03-26T19:00:08.996Z' };
        utilizationStub.resolves(bucketMetrics);

        const metrics = await fetchCapacityMetrics(bucketMd, request, log);

        assert.strictEqual(metrics, bucketMetrics);
        assert(!logWarnSpy.called);
        assert(!logErrorSpy.called);
    });

    it('should resolve with no error and a default date on 404', async () => {
        const error404 = new Error('Not Found');
        error404.response = { status: 404 };
        utilizationStub.rejects(error404);

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
        utilizationStub.rejects(error404);

        const metrics = await fetchCapacityMetrics(bucketMd, request, log);

        assert(metrics && metrics.date instanceof Date, 'metrics should have a Date for date');
        assert(logWarnSpy.calledOnce);
    });

    it('should reject with error on non-404 failures', async () => {
        const error500 = new Error('Internal Server Error');
        error500.response = { status: 500 };
        utilizationStub.rejects(error500);

        await assert.rejects(fetchCapacityMetrics(bucketMd, request, log), err => err === error500);

        assert(logErrorSpy.calledOnce);
        assert.strictEqual(logErrorSpy.getCall(0).args[1].bucket, 'test-bucket');
        assert.strictEqual(logErrorSpy.getCall(0).args[1].statusCode, 500);
        assert(!logWarnSpy.called);
    });

    it('should reject with error on connection errors', async () => {
        const connError = new Error('Connection refused');
        connError.code = 'ECONNREFUSED';
        utilizationStub.rejects(connError);

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
        utilizationStub.rejects(error500);

        await assert.rejects(
            buildVeeamFileData(createRequest(capacityObjectKey), bucketMd, log),
            err => err.code === 500,
        );
    });

    it('should build capacity.xml with SUR metrics date and applied Used/Available', async () => {
        const metricsDate = new Date('2026-03-26T19:00:08.996Z');
        metadataStub.callsArgWith(2, null, bucketMd);
        utilizationStub.resolves({ date: metricsDate, bytesTotal: 100 });

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
        utilizationStub.rejects(error404);

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
        utilizationStub.resolves({ date: new Date(), bytesTotal: 999 });

        const result = await buildVeeamFileData(createRequest(capacityObjectKey), bucketMdWithUsed, log);

        assert(result.xmlContent.includes('<Used>400</Used>'), 'should keep existing Used value');
    });
});

describe('receiveData', () => {
    let log;

    const payload = '0123456789abcdef0123456789abcdef';
    const payloadSha256 = crypto.createHash('sha256').update(payload).digest('base64');
    // crc64nvme of the payload, base64-encoded (same value as in the
    // functional trailing checksum tests)
    const payloadCrc64 = 'skQv82y5rgE=';
    const chunkedBody = digest =>
        '10\r\n0123456789abcdef\r\n' + '10\r\n0123456789abcdef\r\n' + `0\r\nx-amz-checksum-crc64nvme:${digest}\r\n\r\n`;

    const makeRequest = (body, headers, parsedContentLength, streamingV4Params) => {
        const request = new Readable({ read() {} });
        request.headers = headers;
        request.parsedContentLength = parsedContentLength;
        request.streamingV4Params = streamingV4Params;
        process.nextTick(() => {
            request.push(Buffer.from(body));
            request.push(null);
        });
        return request;
    };

    beforeEach(() => {
        log = new DummyRequestLogger();
    });

    it('should return the body of a plain request', async () => {
        const request = makeRequest(payload, { 'x-amz-content-sha256': 'UNSIGNED-PAYLOAD' }, payload.length);
        const data = await receiveData(request, log);
        assert.strictEqual(data, payload);
    });

    it('should validate a matching x-amz-checksum header', async () => {
        const request = makeRequest(
            payload,
            {
                'x-amz-content-sha256': 'UNSIGNED-PAYLOAD',
                'x-amz-checksum-sha256': payloadSha256,
            },
            payload.length,
        );
        const data = await receiveData(request, log);
        assert.strictEqual(data, payload);
    });

    it('should return BadDigest on x-amz-checksum header mismatch', async () => {
        const request = makeRequest(
            payload,
            {
                'x-amz-content-sha256': 'UNSIGNED-PAYLOAD',
                'x-amz-checksum-sha256': crypto.createHash('sha256').update('other').digest('base64'),
            },
            payload.length,
        );
        await assert.rejects(receiveData(request, log), err => err.is.BadDigest);
    });

    it('should decode an unsigned payload with trailing checksum', async () => {
        const body = chunkedBody(payloadCrc64);
        const request = makeRequest(
            body,
            {
                'content-length': `${body.length}`,
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'x-amz-trailer': 'x-amz-checksum-crc64nvme',
                'x-amz-decoded-content-length': `${payload.length}`,
            },
            payload.length,
        );
        const data = await receiveData(request, log);
        assert.strictEqual(data, payload);
    });

    it('should return BadDigest on trailing checksum mismatch', async () => {
        const body = chunkedBody('AAAAAAAAAAA=');
        const request = makeRequest(
            body,
            {
                'content-length': `${body.length}`,
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'x-amz-trailer': 'x-amz-checksum-crc64nvme',
                'x-amz-decoded-content-length': `${payload.length}`,
            },
            payload.length,
        );
        await assert.rejects(receiveData(request, log), err => err.is.BadDigest);
    });

    it('should reject an unsupported trailing checksum algorithm', async () => {
        const request = makeRequest(
            payload,
            {
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'x-amz-trailer': 'x-amz-checksum-foo',
                'x-amz-decoded-content-length': `${payload.length}`,
            },
            payload.length,
        );
        await assert.rejects(receiveData(request, log), err => err.is.InvalidRequest);
    });

    it('should reject a body exceeding the announced content-length', async () => {
        const request = makeRequest(payload, { 'x-amz-content-sha256': 'UNSIGNED-PAYLOAD' }, 10);
        await assert.rejects(receiveData(request, log), err => err.is.InvalidRequest);
    });

    it('should reject a content-length over the allowed threshold', async () => {
        const request = makeRequest(payload, { 'x-amz-content-sha256': 'UNSIGNED-PAYLOAD' }, 2 * 1024 * 1024);
        await assert.rejects(receiveData(request, log), err => err.is.InvalidInput);
    });

    it('should reject a signed streaming request without v4 params', async () => {
        const request = makeRequest(
            payload,
            {
                'x-amz-content-sha256': 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD',
                'x-amz-decoded-content-length': `${payload.length}`,
            },
            payload.length,
            null,
        );
        await assert.rejects(receiveData(request, log), err => err.is.InvalidArgument);
    });

    describe('with checksums disabled', () => {
        let originalIntegrityChecks;

        beforeEach(() => {
            originalIntegrityChecks = config.integrityChecks;
            config.integrityChecks = { enabled: false };
        });

        afterEach(() => {
            config.integrityChecks = originalIntegrityChecks;
        });

        it('should accept a malformed x-amz-checksum header', async () => {
            // Enabled, this is InvalidRequest.
            const request = makeRequest(
                payload,
                {
                    'x-amz-content-sha256': 'UNSIGNED-PAYLOAD',
                    'x-amz-checksum-crc64nvme': 'not-base64!',
                },
                payload.length,
            );
            const data = await receiveData(request, log);
            assert.strictEqual(data, payload);
        });

        it('should ignore a mismatched x-amz-checksum header', async () => {
            // Enabled, this is BadDigest.
            const request = makeRequest(
                payload,
                {
                    'x-amz-content-sha256': 'UNSIGNED-PAYLOAD',
                    'x-amz-checksum-sha256': crypto.createHash('sha256').update('other').digest('base64'),
                },
                payload.length,
            );
            const data = await receiveData(request, log);
            assert.strictEqual(data, payload);
        });

        it('should still strip an unvalidated trailing checksum from the body', async () => {
            // Enabled, this digest is a BadDigest mismatch.
            const body = chunkedBody('AAAAAAAAAAA=');
            const request = makeRequest(
                body,
                {
                    'content-length': `${body.length}`,
                    'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                    'x-amz-trailer': 'x-amz-checksum-crc64nvme',
                    'x-amz-decoded-content-length': `${payload.length}`,
                },
                payload.length,
            );
            const data = await receiveData(request, log);
            assert.strictEqual(data, payload);
        });
    });
});
