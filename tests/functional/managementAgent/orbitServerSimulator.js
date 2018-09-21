const assert = require('assert');
const { EventEmitter } = require('events');
const url = require('url');
const WebSocket = require('ws');

const logger = require('../../../lib/utilities/logger');
const OrbitSimulatorEvent = {
    EVENT_LISTENING: 'listening',
    EVENT_CONNECTION: 'connection',
    EVENT_MESSAGE: 'message',
};
const {
    ChannelMessageV0,
    MessageType,
} = require('../../../lib/management/ChannelMessageV0');
const {
    pushEndpointUrlFromInstanceId,
} = require('../../../lib/management/push');


class OrbitSimulatorServer extends EventEmitter {
    constructor() {
        super();

        assert(process.env.INITIAL_INSTANCE_ID,
               'INITIAL_INSTANCE_ID env variable is required');
        const instanceId = process.env.INITIAL_INSTANCE_ID;

        const endpointUrl = pushEndpointUrlFromInstanceId(instanceId);
        const endpointUrlObj = url.parse(endpointUrl);

        assert(endpointUrlObj.host, 'localhost');

        this.wss = null;
        this.port = endpointUrlObj.port;
        this.path = endpointUrlObj.path;

        this.stop = this.stop.bind(this);
        process.on('SIGINT', this.stop);
        process.on('SIGHUP', this.stop);
        process.on('SIGQUIT', this.stop);
        process.on('SIGTERM', this.stop);
        process.on('SIGPIPE', () => {});
    }

    start() {
        this.wss = new WebSocket.Server({
            port: this.port,
            path: this.path,
            clientTracking: true,
        });

        this.wss.on('connection', clientWs => {
            clientWs.on('message', data => {
                this.emit(OrbitSimulatorEvent.EVENT_MESSAGE, data);
            });
            this.emit(OrbitSimulatorEvent.EVENT_CONNECTION);
        });
        this.wss.on('listening', () => {
            this.emit(OrbitSimulatorEvent.EVENT_LISTENING);
        });
        this.wss.on('error', error => {
            logger.error('orbit CI simulator error', { error });
        });
    }

    stop(cb) {
        if (!this.wss) {
            if (cb) {
                cb();
            }
            return;
        }
        this.wss.close(() => {
            if (cb) {
                cb();
            }
        });
    }

    sendMessage(messageType, body, channelId) {
        let message = null;

        switch (messageType) {
        case MessageType.CONFIG_OVERLAY_MESSAGE:
            assert(body);
            message = ChannelMessageV0.encodeConfigOverlayMessage(body);
            break;

        case MessageType.METRICS_REQUEST_MESSAGE:
            assert(body);
            message = ChannelMessageV0.encodeMetricsRequestMessage(body);
            break;

        case MessageType.METRICS_REPORT_MESSAGE:
            assert(body);
            message = ChannelMessageV0.encodeMetricsReportMessage(body);
            break;

        case MessageType.CHANNEL_CLOSE_MESSAGE:
            assert(channelId);
            message = ChannelMessageV0.encodeChannelCloseMessage(channelId);
            break;

        case MessageType.CHANNEL_PAYLOAD_MESSAGE:
            assert(body);
            assert(channelId);
            message = ChannelMessageV0.encodeChannelDataMessage(
                channelId,
                Buffer.from(body)
            );
            break;

        default:
            logger.error('unsupported message type', { messageType });
            return;
        }

        this.wss.clients.forEach(client => { client.send(message); });
    }
}

module.exports = {
    OrbitSimulatorServer,
    OrbitSimulatorEvent,
};
