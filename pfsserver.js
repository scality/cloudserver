'use strict'; // eslint-disable-line strict

const arsenal = require('arsenal');
const { config } = require('./lib/Config.js');
const logger = require('./lib/utilities/logger');

const pfsServer = new arsenal.network.rest.RESTServer({
    bindAddress: config.pfsDaemon.bindAddress,
    port: config.pfsDaemon.port,
    dataStore: new arsenal.storage.data.file.DataFileStore({
        dataPath: config.pfsDaemon.dataPath,
        log: config.log,
        isPassthrough: true,
        isReadOnly: config.pfsDaemon.isReadOnly,
    }),
    log: config.log,
});

pfsServer.setup(err => {
    if (err) {
        logger.error('Error initializing REST pfsServer', {
            error: err,
        });
        return;
    }
    pfsServer.start();
});
