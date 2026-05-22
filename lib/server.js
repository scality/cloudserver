const http = require('http');
const https = require('https');
const cluster = require('cluster');
const { series } = require('async');
const arsenal = require('arsenal');
const { setServerHeader } = arsenal.s3routes.routesUtils;
const { RedisClient, StatsClient } = arsenal.metrics;
const monitoringClient = require('./utilities/monitoringHandler');

const logger = require('./utilities/logger');
const { internalHandlers } = require('./utilities/internalHandlers');
const { clientCheck, healthcheckHandler } = require('./utilities/healthcheckHandler');
const { config: _config, serverAccessLogsModes } = require('./Config');
const { blacklistedPrefixes } = require('../constants');
const api = require('./api/api');
const dataWrapper = require('./data/wrapper');
const kms = require('./kms/wrapper');
const locationStorageCheck = require('./api/apiUtils/object/locationStorageCheck');
const vault = require('./auth/vault');
const metadata = require('./metadata/wrapper');
const { initManagement } = require('./management');
const { initManagementClient, isManagementAgentUsed } = require('./management/agentClient');
const { startCleanupJob } = require('./api/apiUtils/rateLimit/cleanup');
const { startRefillJob, stopRefillJob } = require('./api/apiUtils/rateLimit/refillJob');

const HttpAgent = require('agentkeepalive');
const QuotaService = require('./utilization/instance');
const { logServerAccess, setServerAccessLogger, ServerAccessLogger } = require('./utilities/serverAccessLogger');
const { parseLC, MultipleBackendGateway } = arsenal.storage.data;
const websiteEndpoints = _config.websiteEndpoints;
let client = dataWrapper.client;
const implName = dataWrapper.implName;

setServerHeader(_config.serverHeader);

let allEndpoints;
function updateAllEndpoints() {
    allEndpoints = Object.keys(_config.restEndpoints);
}
_config.on('rest-endpoints-update', updateAllEndpoints);
updateAllEndpoints();
_config.on('location-constraints-update', () => {
    if (implName === 'multipleBackends') {
        const clients = parseLC(_config, vault);
        client = new MultipleBackendGateway(clients, metadata, locationStorageCheck);
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
const statsClient = new StatsClient(localCacheClient, STATS_INTERVAL, STATS_EXPIRY);
const enableRemoteManagement = true;

class S3Server {
    /**
     * This represents our S3 connector.
     * @constructor
     * @param {Object} config - Configuration object
     * @param {Worker} [worker=null] - Track the worker when using cluster
     */
    constructor(config, worker) {
        this.config = config;
        this.worker = worker;
        this.cluster = config.isCluster;
        this.servers = [];
        http.globalAgent = new HttpAgent({
            keepAlive: true,
            freeSocketTimeout: arsenal.constants.httpClientFreeSocketTimeout,
        });

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
     * Route requests on 'internal s3' port
     * Same as routeRequest, but ignoring user's bucket policy. This should be used only for
     * backbeat and other internal/system processes that must not be affected by user's bucket
     * policy.
     * Note that this is not a temporary measure: eventually this should be improved to better
     * identify and isolate internal/system calls vs user calls, so that "system" bucket policies
     * may be applied to system requests, and the existing "user" bucket policies apply only to
     * user requests.
     * @param {http.IncomingMessage} req - http request object
     * @param {http.ServerResponse} res - http response object
     * @returns {undefined}
     */
    internalRouteRequest(req, res) {
        req.isInternalServiceRequest = true; // eslint-disable-line no-param-reassign
        req.bypassUserBucketPolicies = true; // eslint-disable-line no-param-reassign
        return this.routeRequest(req, res);
    }

    /**
     * Route requests on 's3' port
     * @param {http.IncomingMessage} req - http request object
     * @param {http.ServerResponse} res - http response object
     * @returns {undefined}
     */
    routeRequest(req, res) {
        monitoringClient.httpActiveRequests.inc();
        const requestStartTime = process.hrtime.bigint();

        // Skip server access logs for heartbeat.
        const isLoggingEnabled =
            _config.serverAccessLogs &&
            (_config.serverAccessLogs.mode === serverAccessLogsModes.LOG_ONLY ||
                _config.serverAccessLogs.mode === serverAccessLogsModes.ENABLED);
        const isInternalRoute = req.url.startsWith('/_');
        const isBackbeatRoute = req.url.startsWith('/_/backbeat/');
        if (isLoggingEnabled && (!isInternalRoute || isBackbeatRoute)) {
            // eslint-disable-next-line no-param-reassign
            req.serverAccessLog = {
                enabled: false,
                startTime: requestStartTime,
                startTimeUnixMS: Date.now(),
            };

            // eslint-disable-next-line no-param-reassign
            res.serverAccessLog = {};

            res.on('finish', () => {
                // eslint-disable-next-line no-param-reassign
                req.serverAccessLog.onFinishEndTime = process.hrtime.bigint();
            });
        }

        // disable nagle algorithm
        req.socket.setNoDelay();
        res.on('close', () => {
            // this is tested by retrieveData
            // eslint-disable-next-line no-param-reassign
            res.isclosed = true;
        });

        const monitorEndOfRequest = () => {
            if (req.serverAccessLog) {
                // eslint-disable-next-line no-param-reassign
                req.serverAccessLog.onCloseEndTime = process.hrtime.bigint();
                logServerAccess(req, res);
            }

            const responseTimeInNs = Number(process.hrtime.bigint() - requestStartTime);
            const labels = {
                method: req.method,
                code: res.statusCode,
            };
            if (req.apiMethod) {
                labels.action = req.apiMethod;
            }
            monitoringClient.httpRequestsTotal.labels(labels).inc();
            monitoringClient.httpRequestDurationSeconds.labels(labels).observe(responseTimeInNs / 1e9);
            monitoringClient.httpActiveRequests.dec();
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
                config: this.config,
                kms,
                metadata,
                locStorageCheckFn: locationStorageCheck,
                vault,
            },
        };
        arsenal.s3routes.routes(req, res, params, logger, this.config);
    }

    /**
     * Route requests on 'admin' port
     * @param {http.IncomingMessage} req - http request object
     * @param {http.ServerResponse} res - http response object
     * @returns {undefined}
     */
    routeAdminRequest(req, res) {
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
        if (reqUids !== undefined && !(/*isValidReqUids*/ (reqUids.length < 128))) {
            // simply ignore invalid id (any user can provide an
            // invalid request ID through a crafted header)
            reqUids = undefined;
        }
        const log =
            reqUids !== undefined ? logger.newRequestLoggerFromSerializedUids(reqUids) : logger.newRequestLogger();
        log.end().addDefaultFields(clientInfo);

        log.debug('received admin request', clientInfo);

        switch (req.url) {
            case '/live':
                healthcheckHandler(clientInfo.clientIP, req, res, log, statsClient, false);
                break;
            case '/ready':
                healthcheckHandler(clientInfo.clientIP, req, res, log, statsClient, true);
                break;

            default:
                monitoringClient.monitoringHandler(clientInfo.clientIP, req, res, log);
                break;
        }
    }

    /**
     * This starts the http server.
     * @param {http.RequestListener} listener - Callback to handle requests
     * @param {number} port - Port to listen on
     * @param {string | undefined} ipAddress - IpAddress to listen on
     * @returns {undefined}
     */
    _startServer(listener, port, ipAddress) {
        // Todo: http.globalAgent.maxSockets, http.globalAgent.maxFreeSockets
        let server;
        if (this.config.https) {
            server = https.createServer({
                cert: this.config.https.cert,
                key: this.config.https.key,
                ca: this.config.https.ca,
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

        // Starting NodeJS v18, the default timeout, when `undefined`, is
        // 5 minutes. We must set the value to zero to allow for long
        // upload durations.
        server.requestTimeout = 0; // disabling request timeout

        server.on('connection', socket => {
            socket.on('error', err => logger.info('request rejected', { error: err }));
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
        // Stop token refill job if running
        if (this.config.rateLimiting?.enabled) {
            stopRefillJob(logger);
        }
        Promise.all(this.servers.map(server => new Promise(resolve => server.close(resolve)))).then(() =>
            process.exit(0),
        );
    }

    caughtExceptionShutdown() {
        if (!this.cluster) {
            process.exit(1);
        }
        logger.error('shutdown of worker due to exception', {
            workerId: this.worker ? this.worker.id : undefined,
            workerPid: this.worker ? this.worker.process.pid : undefined,
        });
        // Will close all servers, cause disconnect event on primary and kill
        // worker process with 'SIGTERM'.
        if (this.worker) {
            this.worker.kill();
        }
    }

    startServer(listenOn, port, routeRequest) {
        if (listenOn.length > 0) {
            listenOn.forEach(item => {
                this._startServer(routeRequest.bind(this), item.port, item.ip);
            });
        } else if (port) {
            this._startServer(routeRequest.bind(this), port);
        }
    }

    initiateStartup(log) {
        series([next => metadata.setup(next), next => clientCheck(true, log, next)], (err, results) => {
            if (err) {
                log.warn('initial health check failed, delaying startup', {
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

            // Start rate limit background job
            if (this.config.rateLimiting?.enabled) {
                startCleanupJob(logger);
                // Start token refill job for token reservation system
                startRefillJob(logger);
            }

            // Start API server(s)
            this.startServer(this.config.listenOn, this.config.port, this.routeRequest);

            // Start internal API server(s)
            this.startServer(this.config.internalListenOn, this.config.internalPort, this.internalRouteRequest);

            // Start metrics server(s) only if not cluster mode worker
            if (!this.cluster && !this.worker) {
                this.startServer(this.config.metricsListenOn, this.config.metricsPort, this.routeAdminRequest);
            }

            // Start quota service health checks
            if (QuotaService.enabled) {
                QuotaService?.setup(log);
            }

            // TODO this should wait for metadata healthcheck to be ok
            // TODO only do this in cluster master
            if (enableRemoteManagement) {
                if (!isManagementAgentUsed()) {
                    setTimeout(() => {
                        initManagement(logger.newRequestLogger());
                    }, 5000);
                } else {
                    initManagementClient();
                }
            }

            try {
                logger.info('ServerAccessLogger config', { config: _config.serverAccessLogs });
                if (
                    _config.serverAccessLogs.mode === serverAccessLogsModes.LOG_ONLY ||
                    _config.serverAccessLogs.mode === serverAccessLogsModes.ENABLED
                ) {
                    var serverAccessLogger = new ServerAccessLogger(
                        _config.serverAccessLogs.outputFile,
                        _config.serverAccessLogs.highWaterMarkBytes,
                        _config.serverAccessLogs.retryReopenDelayMS,
                        _config.serverAccessLogs.checkFileRotationIntervalMS,
                    );
                    setServerAccessLogger(serverAccessLogger);
                    logger.info('ServerAccessLogger enabled');
                } else {
                    logger.info('ServerAccessLogger disabled');
                }
            } catch (error) {
                logger.error('ServerAccessLogger creation error', error);
            }

            this.started = true;
        });
    }
}

function main() {
    const workers = _config.clusters;
    if (!_config.isCluster) {
        process.env.REPORT_TOKEN = _config.reportToken;
        const server = new S3Server(_config);
        server.initiateStartup(logger.newRequestLogger());
    }
    if (_config.isCluster && cluster.isPrimary) {
        for (let n = 0; n < workers; n++) {
            const worker = cluster.fork();
            logger.info('new worker forked', {
                workerId: worker.id,
                workerPid: worker.process.pid,
            });
        }
        setInterval(() => {
            const len = Object.keys(cluster.workers).length;
            if (len < workers) {
                for (let i = len; i < workers; i++) {
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

        const metricServer = new S3Server(_config);
        metricServer.startServer(_config.metricsListenOn, _config.metricsPort, metricServer.routeAdminRequest);
    }
    if (_config.isCluster && cluster.isWorker) {
        const server = new S3Server(_config, cluster.worker);
        server.initiateStartup(logger.newRequestLogger());
    }

    // Avoid default metrics on cluster primary as it only aggregate workers
    if (!_config.isCluster || cluster.isWorker) {
        monitoringClient.collectDefaultMetrics({ timeout: 5000 });
    }
}

module.exports = main;
module.exports.S3Server = S3Server;
