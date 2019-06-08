const UtapiReindex = require('utapi').UtapiReindex;
const { config } = require('../Config');

const reindexConfig = config.utapi && config.utapi.reindex;
if (reindexConfig && reindexConfig.password === undefined) {
    reindexConfig.password = config.utapi && config.utapi.redis &&
        config.utapi.redis.password;
}
const reindex = new UtapiReindex(reindexConfig);
reindex.start();
