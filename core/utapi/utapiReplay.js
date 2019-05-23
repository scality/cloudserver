const UtapiReplay = require('utapi').UtapiReplay;
const _config = require('../Config').config;

// start utapi server
const replay = new UtapiReplay(_config.utapi);
replay.start();
