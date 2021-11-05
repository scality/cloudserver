const http = require('http');
const https = require('https');
const cluster = require('cluster');
const arsenal = require('arsenal');
const { RedisClient, StatsClient } = require('arsenal').metrics;

const logger = require('./utilities/logger');
const { internalHandlers } = require('./utilities/internalHandlers');
const { clientCheck } = require('./utilities/healthcheckHandler');
const _config = require('./Config').config;
const { blacklistedPrefixes } = require('../constants');
const api = require('./api/api');
const data = require('./data/wrapper');

const routes = arsenal.s3routes.routes;
const websiteEndpoints = _config.websiteEndpoints;

let allEndpoints;
function updateAllEndpoints() {
    allEndpoints = Object.keys(_config.restEndpoints);
}
_config.on('rest-endpoints-update', updateAllEndpoints);
updateAllEndpoints();

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
    }

    routeRequest(req, res) {
        // disable nagle algorithm
        req.socket.setNoDelay();
        res.on('close', () => {
            // this is tested by retrieveData
            // eslint-disable-next-line no-param-reassign
            res.isclosed = true;
        });
        const params = {
            api,
            internalHandlers,
            statsClient,
            allEndpoints,
            websiteEndpoints,
            blacklistedPrefixes,
            dataRetrievalFn: data.get,
        };
        routes(req, res, params, logger);
    }

    /*
     * This starts the http server.
     */
    startup(port, ipAddress) {
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
        server.on('checkExpectation', this.routeRequest);

        server.on('request', this.routeRequest);

        server.on('checkContinue', this.routeRequest);

        server.on('listening', () => {
            const addr = server.address() || {
                address: ipAddress || '[::]',
                port,
            };
            const { address } = addr;
            logger.info('server started', { address, port,
                pid: process.pid, serverIP: address, serverPort: port });
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
        clientCheck(true, log, (err, results) => {
            if (err) {
                log.info('initial health check failed, delaying startup', {
                    error: err,
                    healthStatus: results,
                });
                setTimeout(() => this.initiateStartup(log), 2000);
            } else {
                log.debug('initial health check succeeded');
                if (_config.listenOn.length > 0) {
                    _config.listenOn.forEach(item => {
                        this.startup(item.port, item.ip);
                    });
                } else {
                    this.startup(_config.port);
                }
            }
        });
    }
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
    } else {
        const server = new S3Server(cluster.worker);
        server.initiateStartup(logger.newRequestLogger());
    }
}

module.exports = main;
