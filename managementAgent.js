const WebSocket = require('ws');
const Uuid = require('uuid');

const logger = require('./lib/utilities/logger');
const { initManagement } = require('./lib/management');
const _config = require('./lib/Config').config;

process.env.REPORT_TOKEN = process.env.REPORT_TOKEN
                           || _config.reportToken
                           || Uuid.v4();

setTimeout(() => {
    initManagement(logger.newRequestLogger());
}, 5000);
