'use strict'; // eslint-disable-line strict
const WebSocket = require('ws');
const assert = require('assert');

const logger = require('../../../lib/utilities/logger');
const _config = require('../../../lib/Config').config;
const {
    managementAgentMessageType,
} = require('../../../lib/management/agentClient');
const {
    patchConfiguration,
} = require('../../../lib/management/configuration');
const metadata = require('../../../lib/metadata/wrapper.js');

function createWs(path) {
    const host = _config.managementAgent.host;
    const port = _config.managementAgent.port;
    return new WebSocket(`ws://${host}:${port}/${path || 'watch'}`);
}

describe('Management process', function testSuite() {
    this.timeout(120000);

    it('should setup metada', done => {
        metadata.setup(done);
    });

    it('should not listen on others routes than `watch`', done => {
        const ws = createWs('wrong_path');
        const msg = 'management agent process should not listen this route';

        ws.on('open', () => {
            logger.error('open');
            return done(new Error(msg));
        });
        ws.on('error', error => {
            logger.error('error', { error });
            return done();
        });
    });

    it('should listen on `watch` route', done => {
        const ws = createWs();

        const msg = 'management agent process should listen this route';
        ws.on('open', done);
        ws.on('error', () => { done(new Error(msg)); });
    });

    it('should send the loaded overlay as first message', done => {
        let firstMsgReceived = false;

        const ws = createWs();

        ws.on('close', done);
        ws.on('error', () => { done(new Error('connection error')); });
        ws.on('message', data => {
            if (!firstMsgReceived) {
                firstMsgReceived = true;
            } else {
                return;
            }

            const msg = JSON.parse(data);
            assert.strictEqual(
                msg.messageType, managementAgentMessageType.NEW_OVERLAY
            );
            assert(msg.payload);

            patchConfiguration(msg.payload, logger.newRequestLogger(), done);
        });
    });

    it('should send the new overlay after the first message', done => {
        const ws = createWs();
        let firstMsgReceived = false;

        ws.on('close', done);
        ws.on('error', () => { done(new Error('connection error')); });
        ws.on('message', data => {
            if (!firstMsgReceived) {
                firstMsgReceived = true;
                return;
            }
            const msg = JSON.parse(data);
            assert.strictEqual(
                msg.messageType, managementAgentMessageType.NEW_OVERLAY
            );
            assert(msg.payload);

            patchConfiguration(msg.payload, logger.newRequestLogger(), done);
        });
    });
});
