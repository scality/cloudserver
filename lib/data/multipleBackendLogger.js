const werelogs = require('werelogs');

const logger = new werelogs.Logger('MultipleBackendGateway');

function createLogger(reqUids) {
    return reqUids ?
        logger.newRequestLoggerFromSerializedUids(reqUids) :
        logger.newRequestLogger();
}

module.exports = createLogger;
