'use strict';

const fs = require('fs');

let config = fs.readFileSync('./config.json', { encoding: 'utf-8' });
config = JSON.parse(config);

for (let i=0; i<config.S3.length; i++) {
    require('./lib/indexUtils.js').default.connectToS3(config.S3[i].host, config.S3[i].port, config.S3[i].index);
}

require('./lib/indexUtils.js').default.connectToDB();
