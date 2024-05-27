// Example filename: tracing.js

'use strict';

const { HoneycombSDK } = require('@honeycombio/opentelemetry-node');
const {
  getNodeAutoInstrumentations,
} = require('@opentelemetry/auto-instrumentations-node');
const { IORedisInstrumentation } = require('@opentelemetry/instrumentation-ioredis');

// Uses environment variables named HONEYCOMB_API_KEY and OTEL_SERVICE_NAME
const sdk = new HoneycombSDK({
    instrumentations: [getNodeAutoInstrumentations({
      // We recommend disabling fs automatic instrumentation because
      // it can be noisy and expensive during startup
        '@opentelemetry/instrumentation-fs': {
            enabled: false,
        },
    }),
    new IORedisInstrumentation(),
  ],
});

sdk.start();

