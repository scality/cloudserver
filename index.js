'use strict'; // eslint-disable-line strict

// so that works in CI checks for process.env.IP
if (process.env.CHILL || process.env.IP) {
    process.binding('http_parser')
        .HTTPParser = require('http-parser-js').HTTPParser;
}
require('babel-core/register')();
require('./lib/server.js').default();
