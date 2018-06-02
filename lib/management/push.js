const net = require('net');
const request = require('request');
const WebSocket = require('ws');

const _config = require('../Config').config;
const logger = require('../utilities/logger');
const metadata = require('../metadata/wrapper');

const {
    patchConfiguration,
    saveConfigurationVersion,
} = require('./configuration');

const {
    ChannelMessageV0,
    MessageType,
} = require('./ChannelMessageV0');

const {
    CONFIG_OVERLAY_MESSAGE,
    METRICS_REQUEST_MESSAGE,
    CHANNEL_CLOSE_MESSAGE,
    CHANNEL_PAYLOAD_MESSAGE,
} = MessageType;

const PING_INTERVAL_MS = 10000;
const subprotocols = [ChannelMessageV0.protocolName];

/**
 * Starts background task that updates configuration and pushes stats.
 *
 * Receives pushed Websocket messages on configuration updates, and
 * sends stat messages in response to API sollicitations.
 *
 * @param {string} pushEndpoint API endpoint
 * @param {string} instanceId UUID of this deployment
 * @param {string} token API authentication token
 *
 * @returns {undefined}
 */
function startWSManagementClient(pushEndpoint, instanceId, token) {
    logger.info('connecting to push server');

    const socketsByChannelId = [];
    const headers = {
        'x-instance-authentication-token': token,
    };
    const url = `${pushEndpoint}/${instanceId}/ws`;
    const ws = new WebSocket(url, subprotocols, { headers });
    let pingTimeout = null;

    function sendPing() {
        if (ws.readyState === ws.OPEN) {
            ws.ping();
        }
        pingTimeout = setTimeout(() => ws.terminate(), PING_INTERVAL_MS);
    }

    function initiatePing() {
        clearTimeout(pingTimeout);
        setTimeout(sendPing, PING_INTERVAL_MS);
    }

    function pushStats(options) {
        const fromURL = `http://localhost:${_config.port}/_/report`;
        const fromOptions = {
            headers: {
                'x-scal-report-token': process.env.REPORT_TOKEN,
                'x-scal-report-skip-cache': Boolean(options && options.noCache),
            },
        };
        request(fromURL, fromOptions, (err, response, body) => {
            ws.send(ChannelMessageV0.encodeMetricsReportMessage(body));
        }).json();
    }

    function closeChannel(channelId) {
        const socket = socketsByChannelId[channelId];
        if (socket) {
            socket.destroy();
            delete socketsByChannelId[channelId];
        }
    }

    function receiveChannelData(channelId, payload) {
        let socket = socketsByChannelId[channelId];
        if (!socket) {
            socket = net.createConnection({
                host: 'localhost',
                port: _config.port,
            });

            socket.on('data', data => {
                ws.send(ChannelMessageV0.
                    encodeChannelDataMessage(channelId, data));
            });

            socket.on('connect', () => {
            });

            socket.on('drain', () => {
            });

            socket.on('end', () => {
                socket.destroy();
                socketsByChannelId[channelId] = null;
                ws.send(ChannelMessageV0.encodeChannelCloseMessage(channelId));
            });

            socketsByChannelId[channelId] = socket;
        }
        socket.write(payload);
    }

    function applyAndSaveOverlay(overlay, log) {
        patchConfiguration(overlay, log, err => {
            if (err) {
                log.error('could not apply pushed overlay', {
                    error: err,
                });
                return;
            }
            saveConfigurationVersion(null, overlay, log, err => {
                if (err) {
                    log.error('could not cache overlay version', {
                        error: err,
                    });
                    return;
                }
                log.info('overlay push processed');
            });
        });
    }

    function browserAccessChangeHandler() {
        if (!_config.browserAccessEnabled) {
            socketsByChannelId.forEach(s => s.close());
        }
    }

    ws.on('open', () => {
        logger.info('connected to push server');

        metadata.notifyBucketChange(() => {
            pushStats({ noCache: true });
        });
        _config.on('browser-access-enabled-change', browserAccessChangeHandler);

        initiatePing();
    });

    ws.on('close', () => {
        logger.info('disconnected from push server, reconnecting in 10s');
        metadata.notifyBucketChange(null);
        _config.removeListener('browser-access-enabled-change',
            browserAccessChangeHandler);
        setTimeout(startWSManagementClient, 10000, pushEndpoint,
            instanceId, token);
    });

    ws.on('error', err => {
        logger.error('error from push server connection', {
            error: err,
            errorMessage: err.message,
        });
    });

    ws.on('ping', () => {
        ws.pong();
    });

    ws.on('pong', () => {
        initiatePing();
    });

    ws.on('message', data => {
        const log = logger.newRequestLogger();
        const message = new ChannelMessageV0(data);

        switch (message.getType()) {
        case CONFIG_OVERLAY_MESSAGE:
            applyAndSaveOverlay(JSON.parse(message.getPayload()), log);
            break;
        case METRICS_REQUEST_MESSAGE:
            pushStats();
            break;
        case CHANNEL_CLOSE_MESSAGE:
            closeChannel(message.getChannelNumber());
            break;
        case CHANNEL_PAYLOAD_MESSAGE:
            if (_config.browserAccessEnabled) {
                receiveChannelData(
                    message.getChannelNumber(), message.getPayload());
            }
            break;
        default:
            logger.error('unknown message type from push server',
                { messageType: message.getType() });
        }
    });
}

module.exports = {
    startWSManagementClient,
};
