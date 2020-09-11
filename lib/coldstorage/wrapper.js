const ColdStorageWrapper =
    require('arsenal').storage.coldstorage.ColdStorageWrapper;
const logger = require('../utilities/logger');

const clientName = 'file';
let params;
if (clientName === 'file') {
    params = {};
}

const coldstorage = new ColdStorageWrapper(clientName, params, logger);
module.exports = coldstorage;
