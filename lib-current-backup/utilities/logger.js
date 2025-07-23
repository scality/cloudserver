const { Werelogs } = require('werelogs');

const _config = require('../Config.js').config;

const werelogs = new Werelogs({
    level: _config.log.logLevel,
    dump: _config.log.dumpLevel,
});
const logger = new werelogs.Logger('S3');

module.exports = logger;
