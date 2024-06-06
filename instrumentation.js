const opentelemetry = require('@opentelemetry/sdk-node');
const { ConsoleSpanExporter } = require('@opentelemetry/sdk-trace-node');
const {
  getNodeAutoInstrumentations,
} = require('@opentelemetry/auto-instrumentations-node');
const {
  PeriodicExportingMetricReader,
  ConsoleMetricExporter,
} = require('@opentelemetry/sdk-metrics');
const { Resource } = require('@opentelemetry/resources');
const {
  SEMRESATTRS_SERVICE_NAME,
  SEMRESATTRS_SERVICE_VERSION,
} = require('@opentelemetry/semantic-conventions');

//exporter

const {
  OTLPMetricExporter,
} = require('@opentelemetry/exporter-metrics-otlp-proto');
const {
  OTLPTraceExporter,
} = require('@opentelemetry/exporter-trace-otlp-proto');


const sdk = new opentelemetry.NodeSDK({
    traceExporter: new OTLPTraceExporter({
        // optional - default url is http://localhost:4318/v1/traces
        url: 'http://localhost:4318/v1/traces',
        // optional - collection of custom headers to be sent with each request, empty by default
        headers: {},
    }),
    resource: new Resource({
        [SEMRESATTRS_SERVICE_NAME]: 's3-cloudserver',
        [SEMRESATTRS_SERVICE_VERSION]: '7.70.47',
    }),
    // traceExporter: new ConsoleSpanExporter(),
    // metricReader: new PeriodicExportingMetricReader({
    //     exporter: new ConsoleMetricExporter(),
    // }),
    metricReader: new PeriodicExportingMetricReader({
        exporter: new OTLPMetricExporter({
            // url is optional and can be omitted - default is http://localhost:4318/v1/metrics
            url: 'http://localhost:4318/v1/metrics',
            // an optional object containing custom headers to be sent with each request
            headers: {},
            // an optional limit on pending requests
            concurrencyLimit: 1,
        }),
    }),
    instrumentations: [getNodeAutoInstrumentations({
        // disabling fs automatic instrumentation because
        // it can be noisy and expensive during startup
        '@opentelemetry/instrumentation-fs': {
            enabled: false,
        },
    })],
});

sdk.start();
