
const utapiServer = require('utapi').UtapiServer;
const _config = require('./Config').default;

// start utapi server
export default function main() {
    if (_config.utapi) {
        const fullConfig = Object.assign({}, _config.utapi);
        if (_config.vaultd) {
            Object.assign(fullConfig, { vaultd: _config.vaultd });
        }
        if (_config.https) {
            Object.assign(fullConfig, { https: _config.https });
        }
        utapiServer(fullConfig);
    }
}
