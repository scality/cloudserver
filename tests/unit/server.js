'use strict';

const assert = require('assert');
const sinon = require('sinon');
const http = require('http');
const https = require('https');
const { config: defaultConfig } = require('../../lib/Config');
const { S3Server } = require('../../lib/server');

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
            https: false
        });
        
        // Call _startServer which should set requestTimeout = 0
        server._startServer(() => {}, 8000, '127.0.0.1');
        
        // Verify that requestTimeout was set to 0
        assert.strictEqual(mockServer.requestTimeout, 0);
    });
});
