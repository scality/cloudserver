const { NodeSDK } = require('@opentelemetry/sdk-node');
const { Resource } = require('@opentelemetry/resources');
const { SemanticResourceAttributes } = require('@opentelemetry/semantic-conventions');
const { OTLPTraceExporter } = require('@opentelemetry/exporter-trace-otlp-http');


// Configure OTLP exporter to same endpoint as Beyla with debugging
const exportUrl = process.env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT ||
    'http://otel-collector.default.svc.cluster.local:4318/v1/traces';

const traceExporter = new OTLPTraceExporter({
    url: exportUrl,
    headers: {
        'Content-Type': 'application/json',
    },
});

// Add export success/failure logging
const originalExport = traceExporter.export.bind(traceExporter);
traceExporter.export = function (spans, resultCallback) {
    return originalExport(spans, result => {
        resultCallback(result);
    });
};

const sdk = new NodeSDK({
    resource: new Resource({
        [SemanticResourceAttributes.SERVICE_NAME]: 'connector-cloudserver-mongodb',
        [SemanticResourceAttributes.SERVICE_VERSION]: '1.0.0',
    }),
    traceExporter,
    instrumentations: [],
});

// Start minimal SDK for span export
sdk.start();
