const werelogs = require('werelogs');
const { errors } = require('arsenal');

const _config = require('../../../Config').config;
const { logHelper } = require('../utils');

werelogs.configure({
    level: _config.log.logLevel,
    dump: _config.log.dumpLevel,
});

const logger = new werelogs.Logger('gcpUtil');

class JsonError extends Error {
    constructor(type, code, desc) {
        super(type);
        this.code = code;
        this.description = desc;
        this[type] = true;
    }
}

function jsonRespCheck(err, resp, body, method, callback) {
    if (err) {
        logHelper(logger, 'error',
            `${method}: error in json method`,
            errors.InternalError.customizeDescription('json method failed'));
        return callback(errors.InternalError
            .customizeDescription('error in JSON Request'));
    }
    if (resp.statusCode >= 300) {
        return callback(
            new JsonError(resp.statusMessage, resp.statusCode));
    }
    let res;
    try {
        res = body && typeof body === 'string' ?
            JSON.parse(body) : body;
    } catch (error) { res = undefined; }
    if (res && res.error && res.error.code >= 300) {
        return callback(
            new JsonError(res.error.message, res.error.code));
    }
    return callback(null, res);
}

function getSourceInfo(CopySource) {
    const source =
        CopySource.startsWith('/') ? CopySource.slice(1) : CopySource;
    const sourceArray = source.split(/\/(.+)/);
    const sourceBucket = sourceArray[0];
    const sourceObject = sourceArray[1];
    return { sourceBucket, sourceObject };
}

module.exports = {
    jsonRespCheck,
    getSourceInfo,
    // util objects
    JsonError,
    logger,
};
