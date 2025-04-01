const { config } = require('../Config');
const { ScubaClientImpl } = require('./scuba/wrapper');

const instance = new ScubaClientImpl(config);

module.exports = instance;
