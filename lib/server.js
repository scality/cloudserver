import http from 'http';
import https from 'https';
import cluster from 'cluster';
import arsenal from 'arsenal';

import { logger } from './utilities/logger';
import { clientCheck } from './utilities/healthcheckHandler';
import _config from './Config';
import routes from './routes';

class S3Server {
    /**
     * This represents our S3 connector.
     * @constructor
     * @param {Worker} [worker=null] - Track the worker when using cluster
     */
    constructor(worker) {
        this.worker = worker;
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

        this.server.on('request', (req, res) => {
            // disable nagle algorithm
            req.socket.setNoDelay();
            routes(req, res, logger);
        });

        // https://nodejs.org/dist/latest-v6.x/
        // docs/api/http.html#http_event_checkexpectation
        this.server.on('checkExpectation', (req, res) => {
            // disable nagle algorithm
            req.socket.setNoDelay();
            routes(req, res, logger);
        });

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
    }

    /*
     * This exits the running process properly.
     */
    cleanUp() {
        logger.info('server shutting down');
        this.server.close(() => process.exit(0));
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
        clientCheck(log, (err, results) => {
            if (err) {
                log.warn('initial health check failed, delaying startup', {
                    error: err,
                    healthStatus: results,
                });
                setTimeout(() => this.initiateStartup(log), 2000);
            } else {
                log.info('initial health check succeeded', {
                    healthStatus: results,
                });
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

export default function main() {
    let clusters = _config.clusters || 1;
    if (process.env.S3BACKEND === 'mem') {
        clusters = 1;
    }
    if (cluster.isMaster) {
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
