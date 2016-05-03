import http from 'http';
import cluster from 'cluster';
import { logger } from './utilities/logger';

import Config from './Config';
import routes from './routes';

const _config = new Config();


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
        // Todo: http.globalAgent.maxSockets, http.globalAgent.maxFreeSockets

        this.server = http.createServer((req, res) => {
            // disable nagle algorithm
            req.socket.setNoDelay();
            routes(req, res, logger);
        });
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
}

export default function main() {
    if (_config.clustering && cluster.isMaster && !process.env.S3BACKEND) {
        for (let n = 0; n < _config.clusters; n++) {
            cluster.fork();
        }
        setInterval(() => {
            const len = Object.keys(cluster.workers).length;
            if (len < _config.clusters) {
                for (let i = len; i < _config.clusters; i++) {
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
    } else if (cluster.isWorker) {
        const server = new S3Server(cluster.worker);
        server.startup();
    } else {
        const server = new S3Server;
        server.startup();
    }
}
