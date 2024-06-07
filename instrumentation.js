const opentelemetry = require('@opentelemetry/sdk-node');
const { WebTracerProvider } = require('@opentelemetry/sdk-trace-web');
const { Resource } = require('@opentelemetry/resources');
const { PeriodicExportingMetricReader } = require('@opentelemetry/sdk-metrics');
const { getNodeAutoInstrumentations } = require('@opentelemetry/auto-instrumentations-node');
const { OTLPMetricExporter } = require('@opentelemetry/exporter-metrics-otlp-proto');
const { OTLPTraceExporter } = require('@opentelemetry/exporter-trace-otlp-proto');
const { BatchSpanProcessor } = require('@opentelemetry/sdk-trace-base');

const {
    SEMRESATTRS_SERVICE_NAME,
    SEMRESATTRS_SERVICE_VERSION,
  } = require('@opentelemetry/semantic-conventions');

// Define resource with service name and version
const resource = new Resource({
    [SEMRESATTRS_SERVICE_NAME]: 'cloudserver',
    [SEMRESATTRS_SERVICE_VERSION]: '7.70.47',
});

// OTLP Trace Exporter configuration
const traceExporter = new OTLPTraceExporter({
    url: 'http://localhost:4318/v1/traces',
    headers: {},
});

// Metric Reader configuration
const metricReader = new PeriodicExportingMetricReader({
    exporter: new OTLPMetricExporter({
        url: 'http://localhost:4318/v1/metrics',
        headers: {},
        concurrencyLimit: 1,
    }),
});

// Node SDK configuration
const sdk = new opentelemetry.NodeSDK({
    traceExporter,
    resource,
    metricReader,
    instrumentations: [
        getNodeAutoInstrumentations({
            '@opentelemetry/instrumentation-fs': {
                enabled: false,
            },
        }),
    ],
});

// Additional WebTracerProvider configuration
// This will initialize TracerProvider that will let us create a Tracers
const webTracerProvider = new WebTracerProvider({ resource });
const webSpanProcessor = new BatchSpanProcessor(traceExporter);
webTracerProvider.addSpanProcessor(webSpanProcessor);
webTracerProvider.register();

// Start the Node SDK
sdk.start();
