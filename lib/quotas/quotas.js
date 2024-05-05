const { config } = require('../Config');
const { ScubaClientImpl } = require('./scuba/wrapper');

let instance = null;

switch (config.backends.quota) {
    case 'scuba':
        instance = new ScubaClientImpl(config);
    break;
    default:
        instance = {
            enabled: false,
        };
    break;
}

module.exports = instance;
