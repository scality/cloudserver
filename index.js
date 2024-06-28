'use strict'; // eslint-disable-line strict

require('werelogs').stderrUtils.catchAndTimestampStderr(
    undefined,
    // Do not exit as workers have their own listener that will exit
    // But primary don't have another listener
    require('cluster').isPrimary ? 1 : null,
);

require('./lib/server.js')();
