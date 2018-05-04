const UtapiReplay = require('utapi').UtapiReplay;
const _config = require('../Config').config;

const utapiConfig = _config.utapi &&
    Object.assign({}, _config.utapi, { redis: _config.redis });
const replay = new UtapiReplay(utapiConfig); // start utapi server
replay.start();
