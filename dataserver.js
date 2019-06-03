'use strict'; // eslint-disable-line strict

const arsenal = require('arsenal');
const { config } = require('./lib/Config.js');
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

if (config.backends.data === 'file' ||
    (config.backends.data === 'multiple' &&
     config.backends.metadata !== 'scality')) {
    const dataServer = new arsenal.network.rest.RESTServer(
        { bindAddress: config.dataDaemon.bindAddress,
            port: config.dataDaemon.port,
            dataStore: new arsenal.storage.data.file.DataFileStore(
                { dataPath: config.dataDaemon.dataPath,
                    log: { logLevel: 'trace', dumpLevel: 'error' } }),
            log: { logLevel: 'trace', dumpLevel: 'error' } });
    dataServer.setup(err => {
        if (err) {
            logger.error('Error initializing REST data server',
                         { error: err });
            return;
        }
        dataServer.start();
    });
}
