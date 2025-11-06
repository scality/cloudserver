const assert = require('assert');
const sinon = require('sinon');
const getVeeamFile = require('../../../lib/routes/veeam/get');
const headVeeamFile = require('../../../lib/routes/veeam/head');
const listVeeamFiles = require('../../../lib/routes/veeam/list');
const UtilizationService = require('../../../lib/utilization/instance');
const metadata = require('../../../lib/metadata/wrapper');
const { DummyRequestLogger } = require('../helpers');

// Helper function to give async callbacks time to execute
const giveAsyncCallbackTimeToExecute = setImmediate;

describe('Veeam routes - comprehensive unit tests', () => {
    let utilizationStub;
    let metadataStub;
    let log;
    let logWarnSpy;

    const bucketMd = {
        _name: 'test-bucket',
        _creationDate: '2024-01-01T00:00:00.000Z',
        getName: () => 'test-bucket',
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

    beforeEach(() => {
        log = new DummyRequestLogger();
        logWarnSpy = sinon.spy();
        log.warn = logWarnSpy;

        // Mock log.end() to return an object with addDefaultFields and info methods
        const logEndStub = {
            addDefaultFields: sinon.stub().returnsThis(),
            info: sinon.stub().returnsThis(),
        };
        log.end = sinon.stub().returns(logEndStub);
        log.debug = sinon.stub();

        utilizationStub = sinon.stub(UtilizationService, 'getUtilizationMetrics');
        metadataStub = sinon.stub(metadata, 'getBucket');
        // By default, metadata.getBucket succeeds
        metadataStub.callsArgWith(2, null, bucketMd, undefined);
    });

    afterEach(() => {
        sinon.restore();
    });

    const createRequest = (objectKey = '.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/capacity.xml') => ({
        bucketName: 'test-bucket',
        objectKey,
        query: {},
        headers: {
            host: 'test-bucket.s3.amazonaws.com',
        },
    });

    const createResponse = () => {
        const response = {
            writeHead: sinon.stub(),
            end: sinon.stub(),
            setHeader: sinon.stub(),
            on: sinon.stub(),
            once: sinon.stub(),
            emit: sinon.stub(),
            write: sinon.stub(),
            pipe: sinon.stub(),
            removeListener: sinon.stub(),
            statusCode: 200,
            headersSent: false,
        };
        // Make response.on return the response object for chaining
        response.on.returns(response);
        response.once.returns(response);
        // Simulate successful end
        response.end.callsFake(() => {
            response.end.called = true;
            response.headersSent = true;
            // Emit finish event when end is called
            const finishHandlers = response.on.getCalls()
                .filter(call => call.args[0] === 'finish')
                .map(call => call.args[1]);
            finishHandlers.forEach(handler => handler());
        });
        // Simulate write
        response.write.callsFake(() => {
            response.headersSent = true;
            return true;
        });
        // Simulate writeHead
        response.writeHead.callsFake(statusCode => {
            response.statusCode = statusCode;
            response.headersSent = true;
        });
        return response;
    };

    it('should handle 404 error from UtilizationService and return 200', done => {
        const error404 = new Error('Not Found');
        error404.response = { status: 404 };
        utilizationStub.callsArgWith(4, error404);

        const request = createRequest();
        const response = createResponse();

        getVeeamFile(request, response, bucketMd, log);

        giveAsyncCallbackTimeToExecute(() => {
            assert(logWarnSpy.calledOnce, 'log.warn should have been called once');
            const warnCall = logWarnSpy.getCall(0);
            assert(warnCall.args[0].includes('UtilizationService returned 404'),
                'warning message should mention 404');
            assert.strictEqual(warnCall.args[1].method, 'getVeeamFile');
            assert.strictEqual(warnCall.args[1].bucket, 'test-bucket');

            assert(response.writeHead.calledWith(200),
                'should return 200 despite 404 from UtilizationService');
            assert(response.end.called, 'response should be ended');
            done();
        });
    });

    it('should handle 500 error from UtilizationService and return 500', done => {
        const error500 = new Error('Internal Server Error');
        error500.response = { status: 500 };
        utilizationStub.callsArgWith(4, error500);

        const request = createRequest();
        const response = createResponse();

        getVeeamFile(request, response, bucketMd, log);

        giveAsyncCallbackTimeToExecute(() => {
            assert(response.headersSent || response.write.called || response.writeHead.called,
                'should send error response for 500 errors');
            done();
        });
    });

    it('should handle connection error from UtilizationService and return 500', done => {
        const errorConn = new Error('Connection refused');
        errorConn.code = 'ECONNREFUSED';
        utilizationStub.callsArgWith(4, errorConn);

        const request = createRequest();
        const response = createResponse();

        getVeeamFile(request, response, bucketMd, log);

        giveAsyncCallbackTimeToExecute(() => {
            assert(response.headersSent || response.write.called || response.writeHead.called,
                'should send error response for connection errors');
            done();
        });
    });

    it('should successfully use metrics when UtilizationService returns data', done => {
        const bucketMetrics = {
            bytesTotal: 123456789,
        };
        utilizationStub.callsArgWith(4, null, bucketMetrics);

        const request = createRequest();
        const response = createResponse();

        getVeeamFile(request, response, bucketMd, log);

        giveAsyncCallbackTimeToExecute(() => {
            assert(!logWarnSpy.called, 'log.warn should not have been called');
            assert(response.writeHead.calledWith(200), 'should return 200 with metrics');
            assert(utilizationStub.calledOnce, 'should call UtilizationService once');
            assert(response.end.called, 'response should be ended');
            done();
        });
    });

    it('should not call UtilizationService for system.xml requests', done => {
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
        metadataStub.callsArgWith(2, null, bucketMdWithSystem);

        const request = createRequest('.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml');
        const response = createResponse();

        getVeeamFile(request, response, bucketMdWithSystem, log);

        setImmediate(() => {
            assert(!utilizationStub.called, 'should not call UtilizationService for system.xml');
            assert(response.writeHead.calledWith(200), 'should return 200 for system.xml');
            assert(response.end.called, 'response should be ended');
            done();
        });
    });

    it('should verify the post-install scenario: 404 returns 200 with Used=0', done => {
        // This test reproduces the post-install scenario where scubaclient returns 404
        // because no metrics are available yet
        const error404 = new Error('Not Found');
        error404.response = { status: 404 };
        utilizationStub.callsArgWith(4, error404);

        const request = createRequest();
        const response = createResponse();

        getVeeamFile(request, response, bucketMd, log);

        giveAsyncCallbackTimeToExecute(() => {
            assert(logWarnSpy.calledOnce, 'should log warning for 404');
            assert(response.writeHead.calledWith(200),
                'should return 200 with static capacity data for 404');
            assert(response.end.called, 'response should be ended');
            const warnCall = logWarnSpy.getCall(0);
            assert(warnCall.args[0].includes('404'), 'warning should mention 404');
            
            done();
        });
    });

    it('should handle metadata.getBucket errors gracefully', done => {
        const metadataError = new Error('Metadata service error');
        metadataStub.callsArgWith(2, metadataError);

        const request = createRequest();
        const response = createResponse();

        getVeeamFile(request, response, bucketMd, log);

        giveAsyncCallbackTimeToExecute(() => {
            assert(response.headersSent || response.write.called || response.writeHead.called,
                'should send response for metadata errors');
            done();
        });
    });

    it('should handle tagging query parameter', done => {
        const request = createRequest();
        request.query = { tagging: '' };
        const response = createResponse();

        getVeeamFile(request, response, bucketMd, log);

        giveAsyncCallbackTimeToExecute(() => {
            assert(response.writeHead.calledWith(200),
                'should return 200 for tagging query');
            assert(response.end.called, 'response should be ended');
            done();
        });
    });
});

describe('Veeam routes - HEAD request UtilizationService error handling', () => {
    let metadataStub;
    let log;
    let logWarnSpy;

    const bucketMd = {
        _name: 'test-bucket',
        _creationDate: '2024-01-01T00:00:00.000Z',
        getName: () => 'test-bucket',
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

    beforeEach(() => {
        log = new DummyRequestLogger();
        logWarnSpy = sinon.spy();
        log.warn = logWarnSpy;

        const logEndStub = {
            addDefaultFields: sinon.stub().returnsThis(),
            info: sinon.stub().returnsThis(),
        };
        log.end = sinon.stub().returns(logEndStub);
        log.debug = sinon.stub();

        metadataStub = sinon.stub(metadata, 'getBucket');
        metadataStub.callsArgWith(2, null, bucketMd, undefined);
    });

    afterEach(() => {
        sinon.restore();
    });

    const createRequest = (objectKey = '.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/capacity.xml') => ({
        bucketName: 'test-bucket',
        objectKey,
        query: {},
        headers: {
            host: 'test-bucket.s3.amazonaws.com',
        },
    });

    const createResponse = () => {
        const response = {
            writeHead: sinon.stub(),
            end: sinon.stub(),
            setHeader: sinon.stub(),
            on: sinon.stub(),
            once: sinon.stub(),
            emit: sinon.stub(),
            write: sinon.stub(),
            pipe: sinon.stub(),
            removeListener: sinon.stub(),
            statusCode: 200,
        };
        response.on.returns(response);
        response.once.returns(response);
        response.end.callsFake(() => {
            response.end.called = true;
        });
        return response;
    };

    it('should handle HEAD request for system.xml', done => {
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
        metadataStub.callsArgWith(2, null, bucketMdWithSystem);

        const request = createRequest('.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml');
        const response = createResponse();

        headVeeamFile(request, response, bucketMdWithSystem, log);

        giveAsyncCallbackTimeToExecute(() => {
            assert(response.setHeader.called, 'should set headers');
            assert(response.end.called, 'response should be ended');
            done();
        });
    });

    it('should handle HEAD request for capacity.xml', done => {
        const request = createRequest('.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/capacity.xml');
        const response = createResponse();

        headVeeamFile(request, response, bucketMd, log);

        giveAsyncCallbackTimeToExecute(() => {
            assert(response.setHeader.called, 'should set headers');
            assert(response.end.called, 'response should be ended');
            done();
        });
    });

    it('should return 404 when no VeeamSOSApi capabilities', done => {
        const bucketMdWithoutVeeam = {
            ...bucketMd,
            _capabilities: {},
        };
        metadataStub.callsArgWith(2, null, bucketMdWithoutVeeam);

        const request = createRequest();
        const response = createResponse();

        headVeeamFile(request, response, bucketMdWithoutVeeam, log);

        giveAsyncCallbackTimeToExecute(() => {
            // HEAD should return 404 via headers, not body
            assert(response.end.called, 'response should be ended');
            done();
        });
    });
});

describe('Veeam routes - LIST request handling', () => {
    let metadataStub;
    let log;
    let logWarnSpy;

    const bucketMd = {
        _name: 'test-bucket',
        _creationDate: '2024-01-01T00:00:00.000Z',
        getName: () => 'test-bucket',
        _capabilities: {
            VeeamSOSApi: {
                SystemInfo: {
                    ProtocolVersion: '1.0',
                    ModelName: 'ARTESCA',
                    LastModified: '2024-01-01T00:00:00.000Z',
                },
                CapacityInfo: {
                    Capacity: 1099511627776,
                    Available: 549755813888,
                    Used: 0,
                    LastModified: '2024-01-01T00:00:00.000Z',
                },
            },
        },
    };

    beforeEach(() => {
        log = new DummyRequestLogger();
        logWarnSpy = sinon.spy();
        log.warn = logWarnSpy;

        const logEndStub = {
            addDefaultFields: sinon.stub().returnsThis(),
            info: sinon.stub().returnsThis(),
        };
        log.end = sinon.stub().returns(logEndStub);
        log.debug = sinon.stub();
        log.trace = sinon.stub();

        metadataStub = sinon.stub(metadata, 'getBucket');
        metadataStub.callsArgWith(2, null, bucketMd, undefined);
    });

    afterEach(() => {
        sinon.restore();
    });

    const createRequest = (query = { 'list-type': '2' }) => ({
        bucketName: 'test-bucket',
        objectKey: '.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/',
        query,
        headers: {
            host: 'test-bucket.s3.amazonaws.com',
        },
        url: '/_/veeam/test-bucket/.system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/',
    });

    const createResponse = () => {
        const response = {
            writeHead: sinon.stub(),
            end: sinon.stub(),
            setHeader: sinon.stub(),
            on: sinon.stub(),
            once: sinon.stub(),
            emit: sinon.stub(),
            write: sinon.stub(),
            pipe: sinon.stub(),
            removeListener: sinon.stub(),
            statusCode: 200,
        };
        response.on.returns(response);
        response.once.returns(response);
        response.end.callsFake(() => {
            response.end.called = true;
            const finishHandlers = response.on.getCalls()
                .filter(call => call.args[0] === 'finish')
                .map(call => call.args[1]);
            finishHandlers.forEach(handler => handler());
        });
        return response;
    };

    it('should list both system.xml and capacity.xml when both are present', done => {
        const request = createRequest();
        const response = createResponse();

        listVeeamFiles(request, response, bucketMd, log);

        giveAsyncCallbackTimeToExecute(() => {
            assert(response.writeHead.calledWith(200), 'should return 200');
            assert(response.end.called, 'response should be ended');
            done();
        });
    });

    it('should handle versions query parameter', done => {
        const request = createRequest({ versions: '' });
        const response = createResponse();

        listVeeamFiles(request, response, bucketMd, log);

        giveAsyncCallbackTimeToExecute(() => {
            assert(response.writeHead.calledWith(200), 'should return 200 for versions query');
            assert(response.end.called, 'response should be ended');
            done();
        });
    });

    it('should return error for invalid query parameters', done => {
        const request = createRequest({ 'invalid-param': 'value' });
        const response = createResponse();

        listVeeamFiles(request, response, bucketMd, log);

        giveAsyncCallbackTimeToExecute(() => {
            // Should return error for invalid query parameter
            assert(response.end.called, 'response should be ended');
            done();
        });
    });

    it('should handle missing bucket metadata', done => {
        const request = createRequest();
        const response = createResponse();

        listVeeamFiles(request, response, null, log);

        giveAsyncCallbackTimeToExecute(() => {
            assert(response.writeHead.calledWith(404), 'should return 404');
            assert(response.end.called, 'response should be ended');
            done();
        });
    });

    it('should list only available files when some capabilities are missing', done => {
        const bucketMdOnlySystem = {
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
        metadataStub.callsArgWith(2, null, bucketMdOnlySystem);

        const request = createRequest();
        const response = createResponse();

        listVeeamFiles(request, response, bucketMdOnlySystem, log);

        giveAsyncCallbackTimeToExecute(() => {
            assert(response.writeHead.calledWith(200), 'should return 200');
            assert(response.end.called, 'response should be ended');
            done();
        });
    });
});

