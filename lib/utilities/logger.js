const werelogs = require('werelogs');

const _config = require('../Config.js').config;

werelogs.configure({
    level: _config.log.logLevel,
    dump: _config.log.dumpLevel,
});
const logger = new werelogs.Logger('S3');

module.exports = logger;
