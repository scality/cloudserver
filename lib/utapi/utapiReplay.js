const UtapiReplay = require('utapi').UtapiReplay;
const _config = require('../Config').default;

// start utapi server
export default function main() {
    const replay = new UtapiReplay(_config.utapi);
    replay.start();
}
