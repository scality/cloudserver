const Uuid = require('uuid');
const WebSocket = require('ws');

const logger = require('./lib/utilities/logger');
const { initManagement } = require('./lib/management');
const _config = require('./lib/Config').config;
const { managementAgentMessageType } = require('./lib/management/agentClient');
const { saveConfigurationVersion } = require('./lib/management/configuration');
const {
    CHECK_BROKEN_CONNECTIONS_FREQUENCY_MS,
    WS_STATUS_IDLE,
} = require('./lib/management/constants');


// TODO: auth?
// TODO: werelogs with a specific name.


class ManagementAgentServer {
    constructor() {
        this.port = _config.managementAgent.port || 8010;
        this.wss = null;
        this.loadedOverlay = null;

        this.stop = this.stop.bind(this);
        process.on('SIGINT', this.stop);
        process.on('SIGHUP', this.stop);
        process.on('SIGQUIT', this.stop);
        process.on('SIGTERM', this.stop);
        process.on('SIGPIPE', () => {});
    }

    start(_cb) {
        const cb = _cb || function noop() {};

        /* Define REPORT_TOKEN env variable needed by the management
         * module. */
        process.env.REPORT_TOKEN = process.env.REPORT_TOKEN
          || _config.reportToken
          || Uuid.v4();

        /* The initManegement function retries when it fails. */
        const log = logger.newRequestLogger();
        return initManagement(log, this.onNewOverlay.bind(this), overlay => {
            let error = null;

            if (overlay) {
                this.loadedOverlay = overlay;
                this.startServer();
            } else {
                error = new Error('failed to init management');
            }
            return cb(error);
        });
    }

    stop() {
        if (!this.wss) {
            process.exit(0);
            return;
        }
        this.wss.close(() => {
            logger.info('server shutdown');
            process.exit(0);
        });
    }

    startServer() {
        this.wss = new WebSocket.Server({
            port: this.port,
            clientTracking: true,
            path: '/watch',
        });

        this.wss.on('connection', this.onConnection.bind(this));
        this.wss.on('listening', this.onListening.bind(this));
        this.wss.on('error', this.onError.bind(this));

        setInterval(this.checkBrokenConnections.bind(this),
                    CHECK_BROKEN_CONNECTIONS_FREQUENCY_MS);
    }

    onConnection(socket, request) {
        function hearthbeat() {
            this.isAlive = true;
        }
        logger.info('client connected to watch route', {
            ip: request.connection.remoteAddress,
        });

        /* eslint-disable no-param-reassign */
        socket.isAlive = true;
        socket.on('pong', hearthbeat.bind(socket));

        if (socket.readyState !== socket.OPEN) {
            logger.error('client socket not in ready state', {
                state: socket.readyState,
                client: socket._socket._peername,
            });
            return;
        }

        const msg = {
            messageType: managementAgentMessageType.NEW_OVERLAY,
            payload: this.loadedOverlay,
        };
        socket.send(JSON.stringify(msg), error => {
            if (error) {
                logger.error('failed to send remoteOverlay to client', {
                    error,
                    client: socket._socket._peername,
                });
            }
        });
    }

    onListening() {
        logger.info('websocket server listening',
                    { port: this.port });
    }

    onError(error) {
        logger.error('websocket server error', { error });
    }

    _sendNewOverlayToClient(client) {
        if (client.readyState !== client.OPEN) {
            logger.error('client socket not in ready state', {
                state: client.readyState,
                client: client._socket._peername,
            });
            return;
        }

        const msg = {
            messageType: managementAgentMessageType.NEW_OVERLAY,
            payload: this.loadedOverlay,
        };
        client.send(JSON.stringify(msg), error => {
            if (error) {
                logger.error(
                  'failed to send remoteOverlay to management agent client', {
                      error, client: client._socket._peername,
                  });
            }
        });
    }

    onNewOverlay(remoteOverlay) {
        const remoteOverlayObj = JSON.parse(remoteOverlay);
        saveConfigurationVersion(
            this.loadedOverlay, remoteOverlayObj, logger, err => {
                if (err) {
                    logger.error('failed to save remote overlay', { err });
                    return;
                }
                this.loadedOverlay = remoteOverlayObj;
                this.wss.clients.forEach(
                    this._sendNewOverlayToClient.bind(this)
                );
            });
    }

    checkBrokenConnections() {
        this.wss.clients.forEach(client => {
            if (!client.isAlive) {
                logger.info('close broken connection', {
                    client: client._socket._peername,
                });
                client.close(WS_STATUS_IDLE.code, WS_STATUS_IDLE.reason);
                return;
            }
            client.isAlive = false;
            client.ping();
        });
    }
}

const server = new ManagementAgentServer();
server.start();
