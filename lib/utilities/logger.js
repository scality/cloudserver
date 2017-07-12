const werelogs = require('werelogs');

const _config = require('../Config').config;

const logger = new werelogs.Logger('S3', {
    level: _config.log.logLevel,
    dump: _config.log.dumpLevel,
});


module.exports = logger;
