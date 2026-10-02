const arsenal = require('@scality/arsenal');
const { HttpsProxyAgent } = require('https-proxy-agent');
const request = require('../utilities/request');
const { URL: _URL } = require('url');
const WebSocket = require('ws');
const http = require('http');

const _config = require('../Config').config;
const logger = require('../utilities/logger');
const metadata = require('../metadata/wrapper');

const { reshapeExceptionError } = arsenal.errorUtils;
const { ChannelMessageV0, MessageType } = require('./ChannelMessageV0');

const { METRICS_REQUEST_MESSAGE } = MessageType;

const PING_INTERVAL_MS = 10000;
const subprotocols = [ChannelMessageV0.protocolName];

const cloudServerHost = process.env.SECURE_CHANNEL_DEFAULT_FORWARD_TO_HOST || 'localhost';
const cloudServerPort = process.env.SECURE_CHANNEL_DEFAULT_FORWARD_TO_PORT || _config.port;

let connected = false;

// No wildcard nor cidr/mask match for now
function createWSAgent(pushEndpoint, env, log) {
    const url = new _URL(pushEndpoint);
    const noProxy = (env.NO_PROXY || env.no_proxy || '').split(',');

    if (noProxy.includes(url.hostname)) {
        log.info('push server ws has proxy exclusion', { noProxy });
        return null;
    }

    if (url.protocol === 'https:' || url.protocol === 'wss:') {
        const httpsProxy = env.HTTPS_PROXY || env.https_proxy;
        if (httpsProxy) {
            log.info('push server ws using https proxy', { httpsProxy });
            return new HttpsProxyAgent(httpsProxy);
        }
    } else if (url.protocol === 'http:' || url.protocol === 'ws:') {
        const httpProxy = env.HTTP_PROXY || env.http_proxy;
        if (httpProxy) {
            log.info('push server ws using http proxy', { httpProxy });
            return new HttpsProxyAgent(httpProxy);
        }
    }

    const allProxy = env.ALL_PROXY || env.all_proxy;
    if (allProxy) {
        log.info('push server ws using wildcard proxy', { allProxy });
        return new HttpsProxyAgent(allProxy);
    }

    log.info('push server ws not using proxy');
    return null;
}

/**
 * Starts background task that pushes stats to the management API.
 *
 * Sends the /_/report metrics in response to API sollicitations and on
 * bucket changes.
 *
 * @param {string} url API endpoint
 * @param {string} token API authentication token
 * @param {function} cb end-of-connection callback
 *
 * @returns {undefined}
 */
function startWSManagementClient(url, token, cb) {
    logger.info('connecting to push server', { url });
    function _logError(error, errorMessage, method) {
        if (error) {
            logger.error(`management client error: ${errorMessage}`, { error: reshapeExceptionError(error), method });
        }
    }

    const headers = {
        'x-instance-authentication-token': token,
    };
    const agent = createWSAgent(url, process.env, logger);

    const ws = new WebSocket(url, subprotocols, { headers, agent });
    let pingTimeout = null;

    function sendPing() {
        if (ws.readyState === ws.OPEN) {
            ws.ping(err => _logError(err, 'failed to send a ping', 'sendPing'));
        }
        pingTimeout = setTimeout(() => ws.terminate(), PING_INTERVAL_MS);
    }

    function initiatePing() {
        clearTimeout(pingTimeout);
        setTimeout(sendPing, PING_INTERVAL_MS);
    }

    function pushStats(options) {
        if (process.env.PUSH_STATS === 'false') {
            return;
        }
        const fromURL = `http://${cloudServerHost}:${cloudServerPort}/_/report`;
        const fromOptions = {
            json: true,
            headers: {
                'x-scal-report-token': process.env.REPORT_TOKEN,
                'x-scal-report-skip-cache': Boolean(options && options.noCache),
            },
        };
        request.get(fromURL, fromOptions, (err, response, body) => {
            if (err) {
                _logError(err, 'failed to get metrics report', 'pushStats');
                return;
            }
            ws.send(ChannelMessageV0.encodeMetricsReportMessage(body), err =>
                _logError(err, 'failed to send metrics report message', 'pushStats'),
            );
        });
    }

    ws.on('open', () => {
        connected = true;
        logger.info('connected to push server');

        metadata.notifyBucketChange(() => {
            pushStats({ noCache: true });
        });

        initiatePing();
    });

    const cbOnce = cb ? arsenal.jsutil.once(cb) : null;

    ws.on('close', () => {
        logger.info('disconnected from push server, reconnecting in 10s');
        metadata.notifyBucketChange(null);
        setTimeout(startWSManagementClient, 10000, url, token);
        connected = false;

        if (cbOnce) {
            process.nextTick(cbOnce);
        }
    });

    ws.on('error', err => {
        connected = false;
        logger.error('error from push server connection', {
            error: err,
            errorMessage: err.message,
        });
        if (cbOnce) {
            process.nextTick(cbOnce, err);
        }
    });

    ws.on('ping', () => {
        ws.pong(err => _logError(err, 'failed to send a pong'));
    });

    ws.on('pong', () => {
        initiatePing();
    });

    ws.on('message', data => {
        const message = new ChannelMessageV0(data);
        if (message.getType() === METRICS_REQUEST_MESSAGE) {
            pushStats();
        } else {
            logger.debug('ignoring message from push server', { messageType: message.getType() });
        }
    });
}

function startPushConnectionHealthCheckServer(cb) {
    const server = http.createServer((req, res) => {
        if (req.url !== '/_/healthcheck') {
            res.writeHead(404);
            res.write('Not Found');
        } else if (connected) {
            res.writeHead(200);
            res.write('Connected');
        } else {
            res.writeHead(503);
            res.write('Not Connected');
        }
        res.end();
    });

    server.listen(_config.port, cb);
}

module.exports = {
    createWSAgent,
    startWSManagementClient,
    startPushConnectionHealthCheckServer,
};
