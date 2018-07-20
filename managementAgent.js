const logger = require('./lib/utilities/logger');
const { initManagement } = require('./lib/management');
const { addConfigurationListener } = require('./lib/management/configuration');
const _config = require('./lib/Config').config;
const WebSocket = require('ws');


//TODO: signal handler to cleanly shutdown server
//TODO: ping messages between server and client
//TODO: auth?

/* Define REPORT_TOKEN env variable needed by the management module. */
process.env.REPORT_TOKEN = process.env.REPORT_TOKEN
                           || _config.reportToken
                           || 'management_agent_report_token';

setTimeout(() => {
    initManagement(logger.newRequestLogger());
}, 5000);


const port = _config.managementAgent.port || 8010;
const wss = new WebSocket.Server({
    port: port,
    clientTracking: true,
    path: '/watch'
});

wss.on('connection', ws => {
    logger.info('management agent: client connected to watch route');
});

wss.on('listening', ws => {
    logger.info('management agent websocket server listening', { port });
});

wss.on('error', error => {
    logger.error('management agent websocket server error', { error });
});

addConfigurationListener((remoteOverlay) => {
    wss.clients.forEach((client) => {
        const msg = {
            messageType: 'NEW_OVERLAY_VERSION',
            payload: remoteOverlay
        };
        client.send(JSON.stringify(msg), (error) => {
            if (error) {
                logger.error('failed to send remoteOverlay to client',
                             { error, client });
            }
        });
    });
});
