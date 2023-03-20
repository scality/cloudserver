const http = require('http');
const https = require('https');
const cluster = require('cluster');
const { series } = require('async');
const arsenal = require('arsenal');
const { RedisClient, StatsClient } = arsenal.metrics;
const monitoringClient = require('./utilities/monitoringHandler');
const metrics = require('./utilities/metrics');

const logger = require('./utilities/logger');
const { internalHandlers } = require('./utilities/internalHandlers');
const { clientCheck, healthcheckHandler } = require('./utilities/healthcheckHandler');
const _config = require('./Config').config;
const { blacklistedPrefixes } = require('../constants');
const api = require('./api/api');
const dataWrapper = require('./data/wrapper');
const kms = require('./kms/wrapper');
const locationStorageCheck =
    require('./api/apiUtils/object/locationStorageCheck');
const vault = require('./auth/vault');
const metadata = require('./metadata/wrapper');

const routes = arsenal.s3routes.routes;
const { parseLC, MultipleBackendGateway } = arsenal.storage.data;
const websiteEndpoints = _config.websiteEndpoints;
let client = dataWrapper.client;
const implName = dataWrapper.implName;

let allEndpoints;
function updateAllEndpoints() {
    allEndpoints = Object.keys(_config.restEndpoints);
}
_config.on('rest-endpoints-update', updateAllEndpoints);
updateAllEndpoints();
_config.on('location-constraints-update', () => {
    if (implName === 'multipleBackends') {
        const clients = parseLC(_config, vault);
        client = new MultipleBackendGateway(
            clients, metadata, locationStorageCheck);
    }
});

// redis client
let localCacheClient;
if (_config.localCache) {
    localCacheClient = new RedisClient(_config.localCache, logger);
}
// stats client
const STATS_INTERVAL = 5; // 5 seconds
const STATS_EXPIRY = 30; // 30 seconds
const statsClient = new StatsClient(localCacheClient, STATS_INTERVAL,
    STATS_EXPIRY);

class S3Server {
    /**
     * This represents our S3 connector.
     * @constructor
     * @param {Worker} [worker=null] - Track the worker when using cluster
     */
    constructor(worker) {
        this.worker = worker;
        this.servers = [];
        http.globalAgent.keepAlive = true;

        process.on('SIGINT', this.cleanUp.bind(this));
        process.on('SIGHUP', this.cleanUp.bind(this));
        process.on('SIGQUIT', this.cleanUp.bind(this));
        process.on('SIGTERM', this.cleanUp.bind(this));
        process.on('SIGPIPE', () => {});
        // This will pick up exceptions up the stack
        process.on('uncaughtException', err => {
            // If just send the error object results in empty
            // object on server log.
            logger.fatal('caught error', {
                error: err.message,
                stack: err.stack,
                workerId: this.worker ? this.worker.id : undefined,
                workerPid: this.worker ? this.worker.process.pid : undefined,
            });
            this.caughtExceptionShutdown();
        });
        this.started = false;
    }

    /**
     * Route requests on 's3' port
     * @param {http.IncomingMessage} req - http request object
     * @param {http.ServerResponse} res - http response object
     * @returns {void}
     */
    routeRequest(req, res) {
        metrics.httpActiveRequests.inc();
        const requestStartTime = process.hrtime.bigint();

        // disable nagle algorithm
        req.socket.setNoDelay();
        res.on('close', () => {
            // this is tested by retrieveData
            // eslint-disable-next-line no-param-reassign
            res.isclosed = true;
        });

        const monitorEndOfRequest = () => {
            const responseTimeInNs = Number(process.hrtime.bigint() - requestStartTime);
            const labels = {
                method: req.method,
                code: res.statusCode,
            };
            if (req.apiMethod) {
                labels.action = req.apiMethod;
            }
            metrics.httpRequestsTotal.labels(labels).inc();
            metrics.httpRequestDurationSeconds
                .labels(labels)
                .observe(responseTimeInNs / 1e9);
            metrics.httpActiveRequests.dec();
        };
        res.on('close', monitorEndOfRequest);

        // use proxied hostname if needed
        if (req.headers['x-target-host']) {
            // eslint-disable-next-line no-param-reassign
            req.headers.host = req.headers['x-target-host'];
        }

        const params = {
            api,
            internalHandlers,
            statsClient,
            allEndpoints,
            websiteEndpoints,
            blacklistedPrefixes,
            dataRetrievalParams: {
                client,
                implName,
                config: _config,
                kms,
                metadata,
                locStorageCheckFn: locationStorageCheck,
                vault,
            },
        };
        routes(req, res, params, logger, _config);
    }

    /**
     * This starts the http server.
     * @param {http.RequestListener} listener - Callback to handle requests
     * @param {number} port - Port to listen on
     * @param {string | undefined} ipAddress - IpAddress to listen on
     * @returns {void}
     */
    _startServer(listener, port, ipAddress) {
        // Todo: http.globalAgent.maxSockets, http.globalAgent.maxFreeSockets
        let server;
        if (_config.https) {
            server = https.createServer({
                cert: _config.https.cert,
                key: _config.https.key,
                ca: _config.https.ca,
                ciphers: arsenal.https.ciphers.ciphers,
                dhparam: arsenal.https.dhparam.dhparam,
                rejectUnauthorized: true,
            });
            logger.info('Https server configuration', {
                https: true,
            });
        } else {
            server = http.createServer();
            logger.info('Http server configuration', {
                https: false,
            });
        }

        server.on('connection', socket => {
            socket.on('error', err => logger.info('request rejected',
                { error: err }));
        });

        // https://nodejs.org/dist/latest-v6.x/
        // docs/api/http.html#http_event_checkexpectation
        server.on('checkExpectation', listener);
        server.on('request', listener);
        server.on('checkContinue', listener);

        server.on('listening', () => {
            const addr = server.address() || {
                address: ipAddress || '[::]',
                port,
            };
            const { address } = addr;
            logger.info('server started', {
                address,
                port,
                pid: process.pid,
                serverIP: address,
                serverPort: port,
            });
            if (_config.nullVersionCompatMode) {
                logger.warn('null version compatibility mode is enabled');
            }
        });

        if (ipAddress !== undefined) {
            server.listen(port, ipAddress);
        } else {
            server.listen(port);
        }

        this.servers.push(server);
    }

    /*
     * This exits the running process properly.
     */
    cleanUp() {
        logger.info('server shutting down');
        Promise.all(this.servers.map(server =>
            new Promise(resolve => server.close(resolve))
        )).then(() => process.exit(0));
    }

    caughtExceptionShutdown() {
        logger.error('shutdown of worker due to exception', {
            workerId: this.worker ? this.worker.id : undefined,
            workerPid: this.worker ? this.worker.process.pid : undefined,
        });
        // Will close all servers, cause disconnect event on master and kill
        // worker process with 'SIGTERM'.
        this.worker.kill();
    }

    initiateStartup(log) {
        series([
            next => metadata.setup(next),
            next => clientCheck(true, log, next),
        ], (err, results) => {
            if (err) {
                log.info('initial health check failed, delaying startup', {
                    error: err,
                    healthStatus: results,
                });
                setTimeout(() => this.initiateStartup(log), 2000);
                return;
            }
            log.debug('initial health check succeeded');
            if (this.started) {
                return;
            }

            // Start API server(s)
            if (_config.listenOn.length > 0) {
                _config.listenOn.forEach(item => {
                    this._startServer(this.routeRequest, item.port, item.ip);
                });
            } else {
                this._startServer(this.routeRequest, _config.port);
            }

            this.started = true;
        });
    }
}

/**
 * Route requests on 'admin' port
 * @param {http.IncomingMessage} req - http request object
 * @param {http.ServerResponse} res - http response object
 * @returns {void}
 */
function _routeAdminRequest(req, res) {
    // use proxied hostname if needed
    if (req.headers['x-target-host']) {
        // eslint-disable-next-line no-param-reassign
        req.headers.host = req.headers['x-target-host'];
    }

    const clientInfo = {
        clientIP: req.socket.remoteAddress,
        clientPort: req.socket.remotePort,
        httpMethod: req.method,
        httpURL: req.url,
        endpoint: req.endpoint,
    };

    let reqUids = req.headers['x-scal-request-uids'];
    if (reqUids !== undefined && !(reqUids.length < 128)) {
        // simply ignore invalid id (any user can provide an
        // invalid request ID through a crafted header)
        reqUids = undefined;
    }
    const log = (reqUids !== undefined ?
        logger.newRequestLoggerFromSerializedUids(reqUids) :
        logger.newRequestLogger());
    log.end().addDefaultFields(clientInfo);

    log.debug('received admin request', clientInfo);

    switch (req.url) {
    case '/live':
    case '/ready':
        healthcheckHandler(clientInfo.clientIP, req, res, log, statsClient);
        break;

    default:
        monitoringClient.monitoringHandler(clientInfo.clientIP, req, res, log);
        break;
    }
}

function setupMetricsServer() {
    const WebServer = arsenal.network.http.server;
    const metricsServer = new WebServer(_config.metricsPort, logger);
    metricsServer.onRequest(_routeAdminRequest);
    if (_config.https) {
        const { ca, cert, key } = _config.https;
        metricsServer.setHttps(cert, key, ca);
    }
    // start
    logger.info(`starting metrics server on port ${_config.metricsPort}`);
    metricsServer.start();
}

function main() {
    let clusters = _config.clusters || 1;
    if (process.env.S3BACKEND === 'mem') {
        clusters = 1;
    }
    if (cluster.isMaster) {
        // Make sure all workers use the same report token
        process.env.REPORT_TOKEN = _config.reportToken;

        for (let n = 0; n < clusters; n++) {
            const worker = cluster.fork();
            logger.info('new worker forked', {
                workerId: worker.id,
                workerPid: worker.process.pid,
            });
        }
        setInterval(() => {
            const len = Object.keys(cluster.workers).length;
            if (len < clusters) {
                for (let i = len; i < clusters; i++) {
                    const newWorker = cluster.fork();
                    logger.info('new worker forked', {
                        workerId: newWorker.id,
                        workerPid: newWorker.process.pid,
                    });
                }
            }
        }, 1000);
        cluster.on('disconnect', worker => {
            logger.error('worker disconnected. making sure exits', {
                workerId: worker.id,
                workerPid: worker.process.pid,
            });
            setTimeout(() => {
                if (!worker.isDead() && !worker.exitedAfterDisconnect) {
                    logger.error('worker not exiting. killing it', {
                        workerId: worker.id,
                        workerPid: worker.pid,
                    });
                    worker.process.kill('SIGKILL');
                }
            }, 2000);
        });
        cluster.on('exit', worker => {
            logger.error('worker exited.', {
                workerId: worker.id,
                workerPid: worker.process.pid,
            });
        });
        // setup metrics server in master, prom-client will not aggregate metrics
        // from workers by default, use prom-client's aggregate registry to collect
        // from all workers!
        setupMetricsServer();
    } else {
        const server = new S3Server(cluster.worker);
        server.initiateStartup(logger.newRequestLogger());
        // collect default metrics from the workers (will be aggregated in master)
        monitoringClient.collectDefaultMetrics();
    }
}

module.exports = main;
