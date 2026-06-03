'use strict';

require('werelogs').stderrUtils.catchAndTimestampStderr(
    undefined,
    // Do not exit as workers have their own listener that will exit
    // But primary don't have another listener
    require('cluster').isPrimary ? 1 : null,
);

const tracing = require('arsenal/build/lib/tracing');

// Gated on isEnabled() so the OTEL-off path doesn't load Config early.
if (tracing.isEnabled() && !(require('./lib/Config').config.isCluster && require('cluster').isPrimary)) {
    tracing.init({
        serviceName: 'cloudserver',
        serviceVersion: require('./package.json').version,
        instrumentations: () => {
            const { HttpInstrumentation } = require('@opentelemetry/instrumentation-http');
            const { IORedisInstrumentation } = require('@opentelemetry/instrumentation-ioredis');
            const { MongoDBInstrumentation } = require('@opentelemetry/instrumentation-mongodb');
            const healthPaths = ['/live', '/ready', '/_/healthcheck', '/_/healthcheck/deep', '/metrics'];
            return [
                new HttpInstrumentation(tracing.makeHttpInstrumentationConfig({ healthPaths })),
                new IORedisInstrumentation({ requireParentSpan: true }),
                new MongoDBInstrumentation({ enhancedDatabaseReporting: false }),
            ];
        },
    });
}

require('./lib/server.js')();
