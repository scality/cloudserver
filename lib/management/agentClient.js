const WebSocket = require('ws');
const logger = require('../utilities/logger');
const _config = require('../Config').config;
const { applyAndSaveOverlay } = require('./configuration');


const managementAgentMessageType = {
    /** Message that contains the loaded overlay */
    NEW_OVERLAY: 1,
};

const CONNECTION_RETRY_TIMEOUT_MS = 5000;


function initManagementClient() {
    const host = _config.managementAgent.host;
    const port = _config.managementAgent.port;

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
        const log = logger.newRequestLogger();
        const msg = JSON.parse(data);

        if (msg.payload === undefined) {
            log.error('message without payload');
            return;
        }
        if (typeof msg.messageType !== 'number') {
            log.error('messageType is not an integer', {
                type: typeof msg.messageType,
            });
            return;
        }

        switch (msg.messageType) {
        case managementAgentMessageType.NEW_OVERLAY:
            applyAndSaveOverlay(msg.payload, log);
            break;
        default:
            log.error('new overlay message version without payload');
            return;
        }
    });
}

function isManagementAgentUsed() {
    return process.env.MANAGEMENT_USE_AGENT === '1';
}


module.exports = {
    managementAgentMessageType,
    initManagementClient,
    isManagementAgentUsed,
};
