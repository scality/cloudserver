const fs = require('fs');
const path = require('path');

const configpath = path.join(__dirname, '/config.json');
var config = fs.readFileSync(configpath, { encoding: 'utf-8' });
config = JSON.parse(config);

config.port = 8000 + ((config.port - 8000 + 1) % 3);
config.antidote.port = 8187 + ((config.antidote.port - 8187 + 100) % 400);
config.indexServerPort = 7000 + ((config.indexServerPort - 7000 + 1) % 3);

config = JSON.stringify(config);
fs.writeFileSync(configpath, config, { encoding: 'utf-8' });
