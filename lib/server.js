import http from 'http';
import cluster from 'cluster';
import Logger from 'werelogs';
import bunyanLogstash from 'bunyan-logstash';

import Config from './Config';
import routes from './routes';

const _config = new Config();

const logger = new Logger(
    'S3',
    {
        level: _config.log.logLevel,
        dump: _config.log.dumpLevel,
        streams: [
            { stream: process.stdout},
            {
                type: 'raw',
                stream: bunyanLogstash.createStream({
                    host: _config.log.logstash.host,
                    port: _config.log.logstash.port,
                }),
            }
        ],
    }
);
const log = logger.newRequestLogger();

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
    }

    /**
     * This starts the http server.
     */
    startup() {
        // Todo: http.globalAgent.maxSockets, http.globalAgent.maxFreeSockets

        this.server = http.createServer((req, res) => {
            Promise.resolve(routes(req, res, logger)).catch((err) => {
                this.server.close();
                res.statusCode = 500;
                res.setHeader('content-type', 'text/plain');
                res.end(`There was a problem. ${err}`);
            });
        });
        this.server.on('listening', () => {
            const addr = this.server.address() || {
                address: '0.0.0.0',
                port: _config.port,
            };
            log.trace(`Serving web content at ${addr.address}${addr.port}`
                    + ` - PID: ${process.pid}`);
        });
        this.server.listen(_config.port);
    }

    /**
     * This exits the running process properly.
     */
    cleanUp() {
        log.trace("Server shutting down...");
        this.server.close();
        process.exit(0);
    }
}

export default function main() {
    if (_config.clustering && cluster.isMaster && !process.env.S3BACKEND) {
        for (let n = 0; n < _config.clusters; n++) {
            cluster.fork();
        }
        cluster.on('disconnect', worker => {
            log.error(`worker ${worker.id} disconnected. Restarting.`);
            cluster.fork();
        });
    } else if (cluster.isWorker) {
        const server = new S3Server(cluster.worker);
        server.startup();
    } else {
        const server = new S3Server;
        server.startup();
    }
}
