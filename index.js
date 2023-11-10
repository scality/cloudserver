'use strict'; // eslint-disable-line strict

/**
 * Catch uncaught exceptions and add timestamp to aid debugging
 */
process.on('uncaughtException', err => {
    process.stderr.write(`${new Date().toISOString()}: Uncaught exception: \n${err.stack}`);
});

require('./lib/server.js')();
