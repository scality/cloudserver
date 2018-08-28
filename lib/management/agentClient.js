const WebSocket = require('ws');
const arsenal = require('arsenal');

const logger = require('../utilities/logger');
const _config = require('../Config').config;
const { patchConfiguration } = require('./configuration');
const { reshapeExceptionError } = arsenal.errorUtils;


const managementAgentMessageType = {
    /** Message that contains the loaded overlay */
    NEW_OVERLAY: 1,
};

const CONNECTION_RETRY_TIMEOUT_MS = 5000;


function initManagementClient() {
    const { host, port } = _config.managementAgent;

    const ws = new WebSocket(`ws://${host}:${port}/watch`);

    ws.on('open', () => {
        logger.info('connected with management agent');
    });

    ws.on('close', (code, reason) => {
        logger.info('disconnected from management agent', { reason });
        setTimeout(initManagementClient, CONNECTION_RETRY_TIMEOUT_MS);
    });

    ws.on('error', error => {
        logger.error('error on connection with management agent', { error });
    });

    ws.on('message', data => {
        const method = 'initManagementclient::onMessage';
        const log = logger.newRequestLogger();
        let msg;

        if (!data) {
            log.error('message without data', { method });
            return;
        }
        try {
            msg = JSON.parse(data);
        } catch (err) {
            log.error('data is an invalid json', { method, err, data });
            return;
        }

        if (msg.payload === undefined) {
            log.error('message without payload', { method });
            return;
        }
        if (typeof msg.messageType !== 'number') {
            log.error('messageType is not an integer', {
                type: typeof msg.messageType,
                method,
            });
            return;
        }

        switch (msg.messageType) {
        case managementAgentMessageType.NEW_OVERLAY:
            patchConfiguration(msg.payload, log, err => {
                if (err) {
                    log.error('failed to patch overlay', {
                        error: reshapeExceptionError(err),
                        method,
                    });
                }
            });
            return;
        default:
            log.error('new overlay message with unmanaged message type', {
                method,
                type: msg.mmessageType,
            });
            return;
        }
    });
}

module.exports = {
    managementAgentMessageType,
    initManagementClient,
};
