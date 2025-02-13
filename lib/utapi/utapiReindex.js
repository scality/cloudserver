require('werelogs').stderrUtils.catchAndTimestampStderr();
// TODO CLDSRV-610 re-enable utapi
/* eslint-disable */
// const UtapiReindex = require('utapi').UtapiReindex;
const { config } = require('../Config');

// TODO CLDSRV-610 re-enable utapi
if (false) {
    const reindexConfig = config.utapi && config.utapi.reindex;
    if (reindexConfig && reindexConfig.password === undefined) {
        reindexConfig.password = config.utapi && config.utapi.redis
        && config.utapi.redis.password;
    }
    const reindex = new UtapiReindex(reindexConfig);
    reindex.start();    
}
