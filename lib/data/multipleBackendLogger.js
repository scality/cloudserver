const werelogs = require('werelogs');
const { config } = require('../Config');

console.log("log level in MultipleBackendGateway file!!", config.log)
const logger = new werelogs.Logger('MultipleBackendGateway', {
    logLevel: config.log.logLevel,
    dumpLevel: config.log.dumpLevel,
});

function createLogger(reqUids) {
    return reqUids ?
        logger.newRequestLoggerFromSerializedUids(reqUids) :
        logger.newRequestLogger();
}

module.exports = createLogger;
