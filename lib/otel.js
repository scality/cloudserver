const { NodeSDK } = require('@opentelemetry/sdk-node');
const { Resource } = require('@opentelemetry/resources');
const { SemanticResourceAttributes } = require('@opentelemetry/semantic-conventions');
const { OTLPTraceExporter } = require('@opentelemetry/exporter-trace-otlp-http');
const { getNodeAutoInstrumentations } = require('@opentelemetry/auto-instrumentations-node');
const { TraceIdRatioBasedSampler } = require('@opentelemetry/sdk-trace-base');

// Configure OTLP exporter to same endpoint as Beyla with debugging
const exportUrl = process.env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT ||
    'http://otel-collector.default.svc.cluster.local:4318/v1/traces';

const traceExporter = new OTLPTraceExporter({
    url: exportUrl,
    headers: {
        'Content-Type': 'application/json',
    },
});

// Export traces without debug logging to avoid linting issues
const originalExport = traceExporter.export.bind(traceExporter);
traceExporter.export = function (spans, resultCallback) {
    return originalExport(spans, resultCallback);
};

const sdk = new NodeSDK({
    resource: new Resource({
        [SemanticResourceAttributes.SERVICE_NAME]: process.env.OTEL_SERVICE_NAME || 'cloudserver',
        [SemanticResourceAttributes.SERVICE_VERSION]: process.env.OTEL_SERVICE_VERSION || '9.0.20',
        [SemanticResourceAttributes.SERVICE_NAMESPACE]: process.env.OTEL_SERVICE_NAMESPACE || 'scality',
    }),
    traceExporter,
    // Add aggressive sampling to reduce collector load - only trace 1% of requests by default
    sampler: new TraceIdRatioBasedSampler(parseFloat(process.env.OTEL_SAMPLING_RATIO) || 0.01),
    instrumentations: [
        // Enable selective auto-instrumentation to reduce span volume
        getNodeAutoInstrumentations({
            // Disable high-volume instrumentations
            '@opentelemetry/instrumentation-fs': { enabled: false },
            '@opentelemetry/instrumentation-redis': { enabled: false },
            // Keep only essential instrumentations
            '@opentelemetry/instrumentation-http': { enabled: true },
            '@opentelemetry/instrumentation-express': { enabled: true },
            // Enable MongoDB with low-cardinality configuration
            '@opentelemetry/instrumentation-mongodb': {
                enabled: true,
                // Enhance spans with operation details but keep names generic
                enhancedDatabaseReporting: true,
                // Don't include collection names in span names to reduce cardinality
                useCollectionName: false,
                // Don't include query details to avoid cardinality explosion
                captureCommandDetails: false
            },
            // Disable AWS SDK to reduce noise
            '@opentelemetry/instrumentation-aws-sdk': { enabled: false },
        }),
    ],
});

// Start the SDK - this must be done before importing any other modules
sdk.start();

module.exports = { sdk };
