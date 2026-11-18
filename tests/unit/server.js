'use strict';

const assert = require('assert');
const sinon = require('sinon');
const http = require('http');
const https = require('https');
const arsenal = require('@scality/arsenal');
const uuid = require('uuid');
const logger = require('../../lib/utilities/logger');
const { config: defaultConfig } = require('../../lib/Config');
const tracing = require('@scality/arsenal/build/lib/tracing');
const { S3Server } = require('../../lib/server');
const recordStore = require('../../lib/api/apiUtils/notificationDestination/recordStore');

describe('S3Server', () => {
    let server;
    let startServerStub;
    let log;
    let config;

    beforeEach(() => {
        log = logger.newRequestLogger();

        config = {
            ...defaultConfig,
            port: undefined,
            listenOn: [],
            internalPort: undefined,
            internalListenOn: [],
            metricsListenOn: [],
            metricsPort: 8002,
        };
        server = new S3Server(config);

        // Stub the _startServer method to verify it's called correctly
        startServerStub = sinon.stub(server, '_startServer');
    });

    afterEach(() => {
        sinon.restore();
    });

    const waitReady = () =>
        new Promise(resolve => {
            const interval = setInterval(() => {
                if (server.started) {
                    clearInterval(interval);
                    resolve();
                }
            }, 100);
        });

    describe('initiateStartup', () => {
        beforeEach(() => {
            sinon.stub(server, 'routeRequest');
            sinon.stub(server, 'internalRouteRequest');
            sinon.stub(server, 'routeAdminRequest');
        });

        // `sinon` matcher to match when the callback argument actually invokes the expected
        // function
        const wrapperFor = expected =>
            sinon.match(actual => {
                const req = uuid.v4();
                const res = uuid.v4();
                actual(req, res);
                return expected.calledWith(req, res);
            });

        it('should start API server with default port if no listenOn is provided', async () => {
            config.port = 8000;

            server.initiateStartup(log);

            await waitReady();

            assert.strictEqual(startServerStub.callCount, 2);
            assert(startServerStub.calledWith(wrapperFor(server.routeRequest), 8000));
            assert(startServerStub.calledWith(wrapperFor(server.routeAdminRequest)));
        });

        it('should start API servers from listenOn array', async () => {
            config.listenOn = [
                { port: 8000, ip: '127.0.0.1' },
                { port: 8001, ip: '0.0.0.0' },
            ];
            config.port = 9999; // Should be ignored since listenOn is provided

            server.initiateStartup(log);

            await waitReady();

            assert.strictEqual(startServerStub.callCount, 3);
            assert(startServerStub.calledWith(wrapperFor(server.routeRequest), 8000, '127.0.0.1'));
            assert(startServerStub.calledWith(wrapperFor(server.routeRequest), 8001, '0.0.0.0'));
            assert(startServerStub.calledWith(wrapperFor(server.routeAdminRequest)));
            assert.strictEqual(startServerStub.neverCalledWith(sinon.any, 9999), true);
        });

        it('should start internal API server with internalPort if no internalListenOn is provided', async () => {
            config.internalPort = 9000;

            server.initiateStartup(log);

            await waitReady();

            assert.strictEqual(startServerStub.callCount, 2);
            assert(startServerStub.calledWith(wrapperFor(server.internalRouteRequest), 9000));
        });

        it('should start internal API servers from internalListenOn array', async () => {
            config.internalListenOn = [
                { port: 9000, ip: '127.0.0.1' },
                { port: 9001, ip: '0.0.0.0' },
            ];
            config.internalPort = 9999; // Should be ignored since internalListenOn is provided

            server.initiateStartup(log);

            await waitReady();

            assert.strictEqual(startServerStub.callCount, 3);
            assert(startServerStub.calledWith(wrapperFor(server.internalRouteRequest), 9000, '127.0.0.1'));
            assert(startServerStub.calledWith(wrapperFor(server.internalRouteRequest), 9001, '0.0.0.0'));
            assert(startServerStub.calledWith(wrapperFor(server.routeAdminRequest)));
            assert.strictEqual(startServerStub.neverCalledWith(sinon.any, 9999), true);
        });

        it('should start metrics server with metricsPort if no metricsListenOn is provided', async () => {
            config.metricsPort = 8012;

            server.initiateStartup(log);

            await waitReady();

            assert.strictEqual(startServerStub.callCount, 1);
            assert(startServerStub.calledWith(wrapperFor(server.routeAdminRequest), 8012));
        });

        it('should start metrics servers from metricsListenOn array', async () => {
            config.metricsListenOn = [
                { port: 8002, ip: '127.0.0.1' },
                { port: 8003, ip: '0.0.0.0' },
            ];
            config.metricsPort = 9999; // Should be ignored since metricsListenOn is provided

            server.initiateStartup(log);

            await waitReady();

            assert.strictEqual(startServerStub.callCount, 2);
            assert(startServerStub.calledWith(wrapperFor(server.routeAdminRequest), 8002, '127.0.0.1'));
            assert(startServerStub.calledWith(wrapperFor(server.routeAdminRequest), 8003, '0.0.0.0'));
            assert.strictEqual(startServerStub.neverCalledWith(server.any, 9999), true);
        });

        it('should start all servers with the correct parameters', async () => {
            config.port = 8000;
            config.internalPort = 9000;
            config.metricsPort = 8002;

            server.initiateStartup(log);

            await waitReady();

            assert.strictEqual(startServerStub.callCount, 3);
            assert(startServerStub.calledWith(wrapperFor(server.routeRequest), 8000));
            assert(startServerStub.calledWith(wrapperFor(server.internalRouteRequest), 9000));
            assert(startServerStub.calledWith(wrapperFor(server.routeAdminRequest), 8002));
        });

        describe('destination API startup log', () => {
            const envVar = 'S3_NOTIFICATION_DESTINATION_API_ENABLED';
            const onMessage = 'bucket notification destination API on';
            let savedEnv;

            beforeEach(() => {
                savedEnv = process.env[envVar];
                delete process.env[envVar];
            });

            afterEach(() => {
                if (savedEnv === undefined) {
                    delete process.env[envVar];
                } else {
                    process.env[envVar] = savedEnv;
                }
            });

            const destinationLines = spy =>
                spy.getCalls().filter(call => /notification destination/.test(call.args[0]));

            [
                ['config.json', undefined],
                [envVar, 'true'],
            ].forEach(([from, envValue]) => {
                it(`should log one line from ${from} when the flag is on`, async () => {
                    if (envValue !== undefined) {
                        process.env[envVar] = envValue;
                    }
                    config.notificationDestinationApi = { enabled: true, bucketd: null };
                    // the flag also starts the record store: keep it out of this test
                    sinon.stub(recordStore, 'start');
                    const info = sinon.spy(logger, 'info');
                    const warn = sinon.spy(logger, 'warn');

                    server.initiateStartup(log);

                    await waitReady();

                    const lines = destinationLines(info);
                    assert.strictEqual(lines.length, 1);
                    assert.strictEqual(lines[0].args[0], onMessage);
                    assert.deepStrictEqual(lines[0].args[1], { from });
                    assert.strictEqual(destinationLines(warn).length, 0);
                });
            });

            it('should log no destination line when the flag is off', async () => {
                config.notificationDestinationApi = { enabled: false, bucketd: null };
                const info = sinon.spy(logger, 'info');
                const warn = sinon.spy(logger, 'warn');

                server.initiateStartup(log);

                await waitReady();

                assert.strictEqual(destinationLines(info).length, 0);
                assert.strictEqual(destinationLines(warn).length, 0);
            });
        });

        it('should start the destination record store when the flag is on', async () => {
            const start = sinon.stub(recordStore, 'start');
            config.notificationDestinationApi = { enabled: true, bucketd: null };

            server.initiateStartup(log);

            await waitReady();

            assert.strictEqual(start.calledOnce, true);
            // bucketclient sends the request uids of the logger it is given
            assert.strictEqual(typeof start.firstCall.args[0].getSerializedUids, 'function');
        });

        it('should not start the destination record store when the flag is off', async () => {
            const start = sinon.stub(recordStore, 'start');
            config.notificationDestinationApi = { enabled: false, bucketd: null };

            server.initiateStartup(log);

            await waitReady();

            assert.strictEqual(start.called, false);
        });
    });

    describe('internalRouteRequest', () => {
        const resp = {
            on: () => {},
            setHeader: () => {},
            writeHead: () => {},
            end: () => {},
        };

        let req;

        beforeEach(() => {
            req = {
                headers: {},
                socket: {
                    setNoDelay: () => {},
                },
                url: 'http://localhost:8000',
            };
        });

        afterEach(() => {
            sinon.restore();
        });

        it('should bypass bucket policy for internal requests', () => {
            const routesMock = sinon.stub().callsFake(req => {
                assert(req.bypassUserBucketPolicies);
            });
            sinon.stub(arsenal.s3routes, 'routes').value(routesMock);

            server.internalRouteRequest(req, resp);
            sinon.assert.calledOnce(routesMock);
        });

        it('should bypass bucket policy for routes requests', () => {
            const routesMock = sinon.stub().callsFake(req => {
                assert(!req.bypassUserBucketPolicies);
            });
            sinon.stub(arsenal.s3routes, 'routes').value(routesMock);

            server.routeRequest(req, resp);
            sinon.assert.calledOnce(routesMock);
        });
    });
});

describe('S3Server request timeout', () => {
    let sandbox;
    let mockServer;

    beforeEach(() => {
        sandbox = sinon.createSandbox();

        // Create a mock server to capture the requestTimeout setting
        mockServer = {
            requestTimeout: null,
            on: sandbox.stub(),
            listen: sandbox.stub(),
            address: sandbox.stub().returns({ address: '127.0.0.1', port: 8000 }),
        };

        // Mock server creation to return our mock
        sandbox.stub(http, 'createServer').returns(mockServer);
        sandbox.stub(https, 'createServer').returns(mockServer);
    });

    afterEach(() => {
        sandbox.restore();
    });

    it('should set server.requestTimeout to 0 when starting server', () => {
        const server = new S3Server({
            ...defaultConfig,
            https: false,
        });

        // Call _startServer which should set requestTimeout = 0
        server._startServer(() => {}, 8000, '127.0.0.1');

        // Verify that requestTimeout was set to 0
        assert.strictEqual(mockServer.requestTimeout, 0);
    });
});

describe('S3Server shutdown', () => {
    let server;
    let tracingCloseStub;
    let exitStub;

    beforeEach(() => {
        server = new S3Server({
            ...defaultConfig,
            port: undefined,
            listenOn: [],
            internalPort: undefined,
            internalListenOn: [],
            metricsListenOn: [],
            metricsPort: 8002,
        });
        // S3Server.cleanUp iterates this.servers and closes each — empty
        // array makes the Promise.all in cleanUp resolve immediately.
        server.servers = [];
        // Avoid touching the rateLimiting refill-job teardown path.
        server.config = { ...server.config, rateLimiting: { enabled: false } };

        tracingCloseStub = sinon.stub(tracing, 'close').resolves();
        exitStub = sinon.stub(process, 'exit');
    });

    afterEach(() => {
        sinon.restore();
    });

    it('should flush OTEL via tracing.close before process.exit(0) on cleanUp', async () => {
        await server.cleanUp();
        sinon.assert.callOrder(tracingCloseStub, exitStub);
        assert.strictEqual(tracingCloseStub.callCount, 1);
        assert.strictEqual(exitStub.callCount, 1);
        assert.strictEqual(exitStub.firstCall.args[0], 0);
    });

    it('should flush OTEL on cleanUp even when a server.close errors', async () => {
        const erroringServer = {
            close(cb) {
                cb(new Error('socket already closed'));
            },
        };
        server.servers = [erroringServer];
        await assert.rejects(() => server.cleanUp(), /socket already closed/);
        assert.strictEqual(tracingCloseStub.callCount, 1);
        assert.strictEqual(exitStub.callCount, 1);
        assert.strictEqual(exitStub.firstCall.args[0], 0);
    });

    it('should still exit(0) on cleanUp even if tracing.close rejects', async () => {
        tracingCloseStub.rejects(new Error('flush failed'));
        await assert.rejects(() => server.cleanUp(), /flush failed/);
        assert.strictEqual(tracingCloseStub.callCount, 1);
        assert.strictEqual(exitStub.firstCall.args[0], 0);
    });

    it('should flush OTEL before process.exit(1) on caughtExceptionShutdown (non-cluster)', async () => {
        server.cluster = false;
        await server.caughtExceptionShutdown();
        sinon.assert.callOrder(tracingCloseStub, exitStub);
        assert.strictEqual(tracingCloseStub.callCount, 1);
        assert.strictEqual(exitStub.firstCall.args[0], 1);
    });

    it('should flush OTEL before worker.kill on caughtExceptionShutdown (cluster worker)', async () => {
        const killStub = sinon.stub();
        server.cluster = true;
        server.worker = { id: 1, process: { pid: 12345 }, kill: killStub };

        await server.caughtExceptionShutdown();

        sinon.assert.callOrder(tracingCloseStub, killStub);
        assert.strictEqual(tracingCloseStub.callCount, 1);
        assert.strictEqual(killStub.callCount, 1);
        // The non-cluster process.exit branch is skipped in this path.
        assert.strictEqual(exitStub.callCount, 0);
    });
});
