const vaultclient = require('vaultclient');
const Vault = require('arsenal').auth.Vault;

const { config } = require('../Config');
const backend = require('./in_memory/backend');
const logger = require('../utilities/logger');

let client;
let implName;

if (config.backends.auth === 'mem') {
    client = backend;
    implName = 'vaultMem';
} else {
    const { host, port } = config.vaultd;
    implName = 'vault';
    if (config.https) {
        const { key, cert, ca } = config.https;
        logger.info('vaultclient configuration', {
            host,
            port,
            https: true,
        });
        client = new vaultclient.Client(host, port, true, key, cert, ca);
    } else {
        logger.info('vaultclient configuration', {
            host,
            port,
            https: false,
        });
        client = new vaultclient.Client(host, port);
    }
    if (config.log) {
        client.setLoggerConfig({
            level: config.log.logLevel,
            dump: config.log.dumpLevel,
        });
    }
}

module.exports = new Vault(client, implName);
