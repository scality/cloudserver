const UtapiReindex = require('utapi').UtapiReindex;
const { config } = require('../Config');

const reindex = new UtapiReindex(config.utapi && config.utapi.reindex);
reindex.start();
