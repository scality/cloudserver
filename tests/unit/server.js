'use strict';

const assert = require('assert');
const sinon = require('sinon');
const logger = require('../../lib/utilities/logger');
const { config: defaultConfig } = require('../../lib/Config');
const { S3Server } = require('../../lib/server');

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
            metricsPort: 8002
        };
        server = new S3Server(config);

        // Stub the _startServer method to verify it's called correctly
        startServerStub = sinon.stub(server, '_startServer');
    });

    afterEach(() => {
        sinon.restore();
    });

    const waitReady = () => new Promise(resolve => {
        const interval = setInterval(() => {
            if (server.started) {
                clearInterval(interval);
                resolve();
            }
        }, 100);
    });

    describe('initiateStartup', () => {
        it('should start API server with default port if no listenOn is provided', async () => {
            config.port = 8000;

            server.initiateStartup(log);

            await waitReady();

            assert.strictEqual(startServerStub.callCount, 2);
            assert(startServerStub.calledWith(server.routeRequest, 8000));
            assert(startServerStub.calledWith(server.routeAdminRequest));

        });
        
        it('should start API servers from listenOn array', async () => {
            config.listenOn = [
                { port: 8000, ip: '127.0.0.1' },
                { port: 8001, ip: '0.0.0.0' }
            ];
            config.port = 9999; // Should be ignored since listenOn is provided

            server.initiateStartup(log);

            await waitReady();

            assert.strictEqual(startServerStub.callCount, 3);
            assert(startServerStub.calledWith(server.routeRequest, 8000, '127.0.0.1'));
            assert(startServerStub.calledWith(server.routeRequest, 8001, '0.0.0.0'));
            assert(startServerStub.calledWith(server.routeAdminRequest));
            assert.strictEqual(startServerStub.neverCalledWith(server.routeRequest, 9999), true);
        });
        
        it('should start internal API server with internalPort if no internalListenOn is provided', async () => {
            config.internalPort = 9000;

            server.initiateStartup(log);

            await waitReady();

            assert.strictEqual(startServerStub.callCount, 2);
            assert(startServerStub.calledWith(server.routeRequest, 9000));
        });
        
        it('should start internal API servers from internalListenOn array', async () => {
            config.internalListenOn = [
                { port: 9000, ip: '127.0.0.1' },
                { port: 9001, ip: '0.0.0.0' }
            ];
            config.internalPort = 9999; // Should be ignored since internalListenOn is provided

            server.initiateStartup(log);

            await waitReady();

            assert.strictEqual(startServerStub.callCount, 3);
            assert(startServerStub.calledWith(server.routeRequest, 9000, '127.0.0.1'));
            assert(startServerStub.calledWith(server.routeRequest, 9001, '0.0.0.0'));
            assert(startServerStub.calledWith(server.routeAdminRequest));
            assert.strictEqual(startServerStub.neverCalledWith(server.routeRequest, 9999), true);
        });
        
        it('should start metrics server with metricsPort if no metricsListenOn is provided', async () => {
            config.metricsPort = 8012;

            server.initiateStartup(log);

            await waitReady();
            
            assert.strictEqual(startServerStub.callCount, 1);
            assert(startServerStub.calledWith(server.routeAdminRequest, 8012));
        });
        
        it('should start metrics servers from metricsListenOn array', async () => {
            config.metricsListenOn = [
                { port: 8002, ip: '127.0.0.1' },
                { port: 8003, ip: '0.0.0.0' }
            ];
            config.metricsPort = 9999; // Should be ignored since metricsListenOn is provided

            server.initiateStartup(log);

            await waitReady();
            
            assert.strictEqual(startServerStub.callCount, 2);
            assert(startServerStub.calledWith(server.routeAdminRequest, 8002, '127.0.0.1'));
            assert(startServerStub.calledWith(server.routeAdminRequest, 8003, '0.0.0.0'));
            assert.strictEqual(startServerStub.neverCalledWith(server.routeAdminRequest, 9999), true);
        });

        it('should start all servers with the correct parameters', async () => {
            config.port = 8000;
            config.internalPort = 9000;
            config.metricsPort = 8002;

            server.initiateStartup(log);

            await waitReady();

            assert.strictEqual(startServerStub.callCount, 3);
            assert(startServerStub.calledWith(server.routeRequest, 8000));
            assert(startServerStub.calledWith(server.routeRequest, 9000));
            assert(startServerStub.calledWith(server.routeAdminRequest, 8002));
        });
    });
});
