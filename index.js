'use strict';

require('werelogs').stderrUtils.catchAndTimestampStderr(
    undefined,
    // Do not exit as workers have their own listener that will exit
    // But primary don't have another listener
    require('cluster').isPrimary ? 1 : null,
);

// Initialize OpenTelemetry SDK before everything else
require('./lib/otel.js');

require('./lib/server.js')();
