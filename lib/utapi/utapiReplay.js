require('werelogs').stderrUtils.catchAndTimestampStderr();
const UtapiReplay = require('@scality/utapi').UtapiReplay;
const _config = require('../Config').config;

const utapiConfig = _config.utapi && Object.assign({}, _config.utapi);
const replay = new UtapiReplay(utapiConfig); // start utapi server
replay.start();
