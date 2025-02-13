require('werelogs').stderrUtils.catchAndTimestampStderr();
// TODO CLDSRV-610 re-enable utapi
/* eslint-disable */
// const UtapiReplay = require('utapi').UtapiReplay;
const _config = require('../Config').config;

// TODO CLDSRV-610 re-enable utapi
if (false) {
    const utapiConfig = _config.utapi &&
        Object.assign({}, _config.utapi, { redis: _config.redis });
    const replay = new UtapiReplay(utapiConfig); // start utapi server
    replay.start();
}
