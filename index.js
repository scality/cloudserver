'use strict'; // eslint-disable-line strict

process.binding('http_parser')
    .HTTPParser = require('http-parser-js').HTTPParser;
require('babel-core/register')();
require('./lib/server.js').default();
