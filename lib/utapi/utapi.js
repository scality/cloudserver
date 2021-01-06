const _config = require('../Config').config;
const { utapiVersion, UtapiServer: utapiServer } = require('utapi');

// start utapi server
if (utapiVersion === 1 && _config.utapi) {
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
