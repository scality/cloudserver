const WebSocket = require('ws');
const Uuid = require('uuid');

const logger = require('./lib/utilities/logger');
const { initManagement } = require('./lib/management');
const { addOverlayMessageListener } = require('./lib/management/push');
const _config = require('./lib/Config').config;

process.env.REPORT_TOKEN = process.env.REPORT_TOKEN
                           || _config.reportToken
                           || Uuid.v4();

function managementAgentWS() {
    const port = _config.managementAgent.port || 8010;
    const wss = new WebSocket.Server({
        port,
        clientTracking: true,
        path: '/watch',
    });

    wss.on('connection', () => {
        logger.info('management agent: client connected to watch route');
    });

    wss.on('listening', () => {
        logger.info('management agent websocket server listening', { port });
    });

    wss.on('error', error => {
        logger.error('management agent websocket server error', { error });
    });

    addOverlayMessageListener(remoteOverlay => {
        wss.clients.forEach(client => {
            if (client.readyState !== client.OPEN) {
                logger.warning('client socket not in ready state', { client });
                return;
            }
            logger.info('NEW OVERLAY');
            const msg = {
                messageType: 'NEW_OVERLAY_VERSION',
                payload: remoteOverlay,
            };
            client.send(JSON.stringify(msg), error => {
                if (error) {
                    logger.error('failed to send remoteOverlay to management' +
                                 ' agent client', { error, client });
                }
            });
        });
    });
}

setTimeout(() => {
    initManagement(logger.newRequestLogger());
}, 5000);

setTimeout(() => {
    managementAgentWS();
}, 6);
