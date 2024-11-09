const opentelemetry = require('@opentelemetry/sdk-node');
const { Resource } = require('@opentelemetry/resources');
const { getNodeAutoInstrumentations } = require('@opentelemetry/auto-instrumentations-node');
const { OTLPTraceExporter } = require('@opentelemetry/exporter-trace-otlp-proto');
const { IORedisInstrumentation } = require('@opentelemetry/instrumentation-ioredis');
const { createOmitSampler } = require('./omitSampler');

const {
    ATTR_SERVICE_NAME,
    ATTR_SERVICE_VERSION,
} = require('@opentelemetry/semantic-conventions');

// Define resource with service name and version
const resource = new Resource({
    [ATTR_SERVICE_NAME]: process.env.OTEL_SERVICE_NAME || 'cisco-s3-cloudserver',
    [ATTR_SERVICE_VERSION]: '7.70.47',
});

const collectorHost = process.env.OTEL_COLLECTOR_HOST || 'localhost';
const collectorPort = process.env.OTEL_COLLECTOR_PORT || 4318;
const samplingRatio = parseFloat(process.env.OTEL_SAMPLING_RATIO) || 0.05;
const excludeHealthcheck = process.env.OTEL_EXCLUDE_HEALTHCHECK_AND_METRICS || 'true';

// OTLP Trace Exporter configuration
const traceExporter = new OTLPTraceExporter({
    url: `http://${collectorHost}:${collectorPort}/v1/traces`,
    headers: {},
});

// Node SDK configuration
const sdk = new opentelemetry.NodeSDK({
    traceExporter,
    resource,
    // sampler: new TraceIdRatioBasedSampler(samplingRatio),
    sampler: createOmitSampler(samplingRatio, excludeHealthcheck),
    instrumentations: [
        getNodeAutoInstrumentations({
            '@opentelemetry/instrumentation-fs': {
                enabled: false,
            },
            '@opentelemetry/instrumentation-http': {
                responseHook: (span, operations) => {
                    span.updateName(
                        `${operations.req.protocol} ${operations.req.method} ${operations.req.path.split('&')[0]}`);
                },
            },
        }),
        new IORedisInstrumentation({
            requestHook: (span, { cmdName, cmdArgs }) => {
                span.updateName(`Redis:: ${cmdName.toUpperCase()} cache operation for ${cmdArgs[0].split(':')[0]}`);
            },
        }),
    ],
});

// Start the Node SDK
sdk.start();
