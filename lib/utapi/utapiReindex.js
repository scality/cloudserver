const UtapiReindex = require('utapi').UtapiReindex;
const { config } = require('../Config');

const reindexConfig = config.utapi.reindex ? config.utapi.reindex : config.utapi
const reindex = new UtapiReindex(reindexConfig);
reindex.start();
