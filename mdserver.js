'use strict'; // eslint-disable-line strict

const { config } = require('./lib/Config.js');
const MetadataFileServer =
          require('arsenal').storage.metadata.MetadataFileServer;
const logger = require('./lib/utilities/logger');

process.on('uncaughtException', err => {
    logger.fatal('caught error', {
        error: err.message,
        stack: err.stack,
        workerId: this.worker ? this.worker.id : undefined,
        workerPid: this.worker ? this.worker.process.pid : undefined,
    });
    process.exit(1);
});

if (config.backends.metadata === 'file') {
    const mdServer = new MetadataFileServer(
        { bindAddress: config.metadataDaemon.bindAddress,
            port: config.metadataDaemon.port,
            path: config.metadataDaemon.metadataPath,
            restEnabled: config.metadataDaemon.restEnabled,
            restPort: config.metadataDaemon.restPort,
            recordLog: config.recordLog,
            versioning: { replicationGroupId: config.replicationGroupId },
            log: config.log });
    mdServer.startServer();
}
