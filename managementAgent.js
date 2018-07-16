const logger = require('./lib/utilities/logger');
const { initManagement } = require('./lib/management');
const _config = require('./lib/Config').config;

process.env.REPORT_TOKEN = process.env.REPORT_TOKEN
                           || _config.reportToken
                           || 'management_agent_report_token';

setTimeout(() => {
    initManagement(logger.newRequestLogger());
}, 5000);
