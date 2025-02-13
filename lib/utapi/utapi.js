require('werelogs').stderrUtils.catchAndTimestampStderr();
const _config = require('../Config').config;
// TODO CLDSRV-610 re-enable utapi
/* eslint-disable */
// const { utapiVersion, UtapiServer: utapiServer } = require('utapi');

// start utapi server
// TODO CLDSRV-610 re-enable utapi
if (utapiVersion === 1 && _config.utapi && false) {
    const fullConfig = Object.assign({}, _config.utapi,
        { redis: _config.redis });
    if (_config.vaultd) {
        Object.assign(fullConfig, { vaultd: _config.vaultd });
    }
    if (_config.https) {
        Object.assign(fullConfig, { https: _config.https });
    }
    // copy healthcheck IPs
    if (_config.healthChecks) {
        Object.assign(fullConfig, { healthChecks: _config.healthChecks });
    }
    utapiServer(fullConfig);
}
