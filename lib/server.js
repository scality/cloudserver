const http = require('http');
const https = require('https');
const cluster = require('cluster');
const { series } = require('async');
const arsenal = require('arsenal');
const { RedisClient, StatsClient } = arsenal.metrics;
const monitoringClient = require('./utilities/monitoringHandler');

const logger = require('./utilities/logger');
const { internalHandlers } = require('./utilities/internalHandlers');
const { clientCheck } = require('./utilities/healthcheckHandler');
const _config = require('./Config').config;
const { blacklistedPrefixes } = require('../constants');
const api = require('./api/api');
const dataWrapper = require('./data/wrapper');
const kms = require('./kms/wrapper');
const locationStorageCheck =
    require('./api/apiUtils/object/locationStorageCheck');
const vault = require('./auth/vault');
const metadata = require('./metadata/wrapper');
const { initManagement } = require('./management');
const {
    initManagementClient,
    isManagementAgentUsed,
} = require('./management/agentClient');

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
    if (implName === 'multipleBackend') {
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
const enableRemoteManagement = true;

class S3Server {
    /**
     * This represents our S3 connector.
     * @constructor
     * @param {Worker} [worker=null] - Track the worker when using cluster
     */
    constructor(worker) {
        this.worker = worker;
        this.cluster = true;
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

    routeRequest(req, res) {
        // disable nagle algorithm
        req.socket.setNoDelay();
        res.on('close', () => {
            // this is tested by retrieveData
            // eslint-disable-next-line no-param-reassign
            res.isclosed = true;
        });

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
        routes(req, res, params, logger);
    }

    /*
     * This starts the http server.
     */
    startup(port, ipAddress) {
        // Todo: http.globalAgent.maxSockets, http.globalAgent.maxFreeSockets
        if (_config.https) {
            this.server = https.createServer({
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
            this.server = http.createServer();
            logger.info('Http server configuration', {
                https: false,
            });
        }

        this.server.on('connection', socket => {
            socket.on('error', err => logger.info('request rejected',
                { error: err }));
        });

        // https://nodejs.org/dist/latest-v6.x/
        // docs/api/http.html#http_event_checkexpectation
        this.server.on('checkExpectation', this.routeRequest);

        this.server.on('request', this.routeRequest);

        this.server.on('checkContinue', this.routeRequest);

        this.server.on('listening', () => {
            const addr = this.server.address() || {
                address: ipAddress || '[::]',
                port,
            };
            logger.info('server started', { address: addr.address,
                port: addr.port, pid: process.pid });
        });
        if (ipAddress !== undefined) {
            this.server.listen(port, ipAddress);
        } else {
            this.server.listen(port);
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
    }

    /*
     * This exits the running process properly.
     */
    cleanUp() {
        logger.info('server shutting down');
        this.server.close(() => process.exit(0));
    }

    caughtExceptionShutdown() {
        if (!this.cluster) {
            process.exit(1);
        }
        logger.error('shutdown of worker due to exception', {
            workerId: this.worker ? this.worker.id : undefined,
            workerPid: this.worker ? this.worker.process.pid : undefined,
        });
        // Will close all servers, cause disconnect event on master and kill
        // worker process with 'SIGTERM'.
        if (this.worker) {
            this.worker.kill();
        }
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
            if (_config.listenOn.length > 0) {
                _config.listenOn.forEach(item => {
                    this.startup(item.port, item.ip);
                });
                return;
            }
            if (!this.started) {
                this.startup(_config.port);
                this.started = true;
            }
        });
    }
}

function main() {
    // TODO: change config to use workers prop. name for clarity
    let workers = _config.clusters || 1;
    if (process.env.S3BACKEND === 'mem') {
        workers = 1;
    }
    this.cluster = workers > 1;
    if (!this.cluster) {
        process.env.REPORT_TOKEN = _config.reportToken;
        const server = new S3Server();
        server.initiateStartup(logger.newRequestLogger());
    }
    if (this.cluster && cluster.isMaster) {
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
    }
    if (this.cluster && cluster.isWorker) {
        const server = new S3Server(cluster.worker);
        server.initiateStartup(logger.newRequestLogger());
    }
    monitoringClient.collectDefaultMetrics({ timeout: 5000 });
}

module.exports = main;
