'use strict'; // eslint-disable-line strict

/**
 * Catch uncaught execeptions and add timestamp to aid debugging
 */
process.on('uncaughtException', err => {
    process.stderr.write(`${new Date().toISOString()} ${err.stack}`);
});
require('./lib/server.js')();
