'use strict'; // eslint-disable-line strict

const arsenal = require('arsenal');
const { config } = require('./lib/Config.js');
const logger = require('./lib/utilities/logger');

const fsDataServer = new arsenal.network.rest.RESTServer({
    bindAddress: config.fsDataDaemon.bindAddress,
    port: config.fsDataDaemon.port,
    dataStore: new arsenal.storage.data.file.DataFileStore({
        dataPath: config.fsDataDaemon.dataPath,
        log: config.log,
        isFs: true,
    }),
    log: config.log,
});

fsDataServer.setup(err => {
    if (err) {
        logger.error('Error initializing REST fs data server', {
            error: err,
        });
        return;
    }
    fsDataServer.start();
});
