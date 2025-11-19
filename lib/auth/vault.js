const vaultclient = require('vaultclient');
const { auth } = require('arsenal');

const { config } = require('../Config');
const ChainBackend = auth.backends.chainBackend;
const backend = require('./in_memory/backend');
const logger = require('../utilities/logger');
const Vault = auth.Vault;

let client;
let implName;

function getVaultClient(config) {
    console.log("FFFFF 501 ", config)
    const { host, port } = config.vaultd;
    let vaultClient;

    if (config.https) {
        const { key, cert, ca } = config.https;
        logger.info('vaultclient configuration', {
            host,
            port,
            https: true,
        });
        vaultClient = new vaultclient.Client(host, port, true, key, cert, ca);
    } else {
        logger.info('vaultclient configuration', {
            host,
            port,
            https: false,
        });
        vaultClient = new vaultclient.Client(host, port);
    }

    if (config.log) {
        vaultClient.setLoggerConfig({
            level: config.log.logLevel,
            dump: config.log.dumpLevel,
        });
    }

    return vaultClient;
}

function getMemBackend(config) {
    console.log("FFFFF 502 ", config)

    config.on('authdata-update', () => {
        backend.refreshAuthData(config.authData);
    });
    return backend;
}
console.log("FFFFF 503 ", config)
console.log("FFFFF 504 ", config.backends)
console.log("FFFFF 505 ", config.backends.auth)

switch (config.backends.auth) {
case 'mem':
    implName = 'vaultMem';
    client = getMemBackend(config);
    break;
case 'multiple':
    implName = 'vaultChain';
    client = new ChainBackend('s3', [
        getMemBackend(config),
        getVaultClient(config),
    ]);
    break;
default: // vault
    implName = 'vault';
    client = getVaultClient(config);
}

console.log("FFFFF 506 ", implName)
console.log("FFFFF 507 ", client)

module.exports = new Vault(client, implName);
