import http from 'http';
import https from 'https';
import cluster from 'cluster';
import arsenal from 'arsenal';

import { logger } from './utilities/logger';
import { clientCheck } from './utilities/healthcheckHandler';
import _config from './Config';
import routes from './routes';
import indexClient from './indexClient/indexClient'

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
            logger.fatal('caught error', { error: err.message,
                stack: err.stack });
            this.caughtExceptionShutdown();
        });
    }

    /*
     * This starts the http server.
     */
    startup() {
        indexClient.createPublisher(_config.indexServerPort, logger)
        // Todo: http.globalAgent.maxSockets, http.globalAgent.maxFreeSockets
        if (_config.https) {
            this.server = https.createServer({
                cert: _config.https.cert,
                key: _config.https.key,
                ca: _config.https.ca,
                ciphers: arsenal.https.ciphers.ciphers,
                dhparam: arsenal.https.dhparam.dhparam,
                rejectUnauthorized: true,
            }, (req, res) => {
                // disable nagle algorithm
                req.socket.setNoDelay();
                routes(req, res, logger);
            });
            logger.info('Https server configuration', {
                https: true,
            });
        } else {
            this.server = http.createServer((req, res) => {
                // disable nagle algorithm
                req.socket.setNoDelay();
                routes(req, res, logger);
            });
            logger.info('Http server configuration', {
                https: false,
            });
        }
        this.server.on('listening', () => {
            const addr = this.server.address() || {
                address: '0.0.0.0',
                port: _config.port,
            };
            logger.info('server started', { address: addr.address,
                port: addr.port, pid: process.pid });
        });
        this.server.listen(_config.port);
    }

    /*
     * This exits the running process properly.
     */
    cleanUp() {
        logger.info('server shutting down');
        this.server.close();
        process.exit(0);
    }

    caughtExceptionShutdown() {
        logger.error('shutdown of worker due to exception');
        // Will close all servers, cause disconnect event on master and kill
        // worker process with 'SIGTERM'.
        this.worker.kill();
        const killTimer = setTimeout(() => {
            if (!this.worker.isDead()) {
                this.worker.kill('SIGKILL');
            }
        }, 2000);
        killTimer.unref();
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
                this.startup();
            }
        });
    }
}

export default function main() {
    let clusters = _config.clusters || 1;
    if (process.env.S3BACKEND === 'mem' || process.env.S3BACKEND === 'antidote') {
        clusters = 1;
    }
    if (cluster.isMaster) {
        for (let n = 0; n < clusters; n++) {
            cluster.fork();
        }
        setInterval(() => {
            const len = Object.keys(cluster.workers).length;
            if (len < clusters) {
                for (let i = len; i < clusters; i++) {
                    const newWorker = cluster.fork();
                    logger.error('new worker forked',
                    { workerId: newWorker.id });
                }
            }
        }, 1000);
        cluster.on('disconnect', worker => {
            logger.error('worker disconnected. making sure exits',
                { workerId: worker.id });
            setTimeout(() => {
                if (!worker.isDead()) {
                    logger.error('worker not exiting. killing it');
                    worker.process.kill('SIGKILL');
                }
            }, 2000);
        });
        cluster.on('exit', worker => {
            logger.error('worker exited.',
                { workerId: worker.id });
        });
    } else {
        const server = new S3Server(cluster.worker);
        server.initiateStartup(logger.newRequestLogger());
    }
}
