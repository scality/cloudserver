'use strict'; // eslint-disable-line strict

const arsenal = require('arsenal');
const { config } = require('./lib/Config.js');
const logger = require('./lib/utilities/logger');

if (config.backends.data === 'file' ||
    (config.backends.data === 'multiple' &&
     config.backends.metadata !== 'scality' &&
     config.backends.metadata !== 'mongodb')) {
    const dataServer = new arsenal.network.rest.RESTServer(
        { bindAddress: config.dataDaemon.bindAddress,
            port: config.dataDaemon.port,
            dataStore: new arsenal.storage.data.file.DataFileStore(
                { dataPath: config.dataDaemon.dataPath,
                    log: config.log,
                    noSync: true }),
            log: config.log });
    dataServer.setup(err => {
        if (err) {
            logger.error('Error initializing REST data server',
                         { error: err });
            return;
        }
        dataServer.start();
    });
}
