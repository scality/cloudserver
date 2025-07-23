const { propagation, trace, context, SpanKind, TraceFlags } = require('@opentelemetry/api');
const { NodeSDK } = require('@opentelemetry/sdk-node');
const { Resource } = require('@opentelemetry/resources');
const { SemanticResourceAttributes } = require('@opentelemetry/semantic-conventions');
const { OTLPTraceExporter } = require('@opentelemetry/exporter-trace-otlp-http');

// Initialize proper OpenTelemetry SDK for span creation
console.log('[OTEL] Initializing OpenTelemetry SDK with explicit OTLP HTTP exporter...');

// Configure OTLP HTTP exporter explicitly
const traceExporter = new OTLPTraceExporter({
    url: process.env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT || 'http://otel-collector.default.svc.cluster.local:4318/v1/traces',
    headers: {
        'Content-Type': 'application/json',
    },
});

const sdk = new NodeSDK({
    resource: new Resource({
        [SemanticResourceAttributes.SERVICE_NAME]: 'connector-cloudserver',
        [SemanticResourceAttributes.SERVICE_VERSION]: '1.0.0',
    }),
    traceExporter: traceExporter,
});

// Start the SDK
sdk.start();

const tracer = trace.getTracer('connector-cloudserver', '1.0.0');

console.log('[OTEL] ✅ OpenTelemetry SDK initialized with explicit OTLP HTTP exporter');
console.log('[OTEL] 🚀 OTLP endpoint:', process.env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT || 'http://otel-collector.default.svc.cluster.local:4318/v1/traces');

// Monkey-patch http and https for header propagation only
const http = require('http');
const https = require('https');

function ensureTraceHeaders(options) {
    if (!options.headers) {
        options.headers = {};
    }
    
    // Enhanced logging for all HTTP requests
    const url = `${options.protocol || 'http:'}//${options.host || options.hostname}:${options.port || 80}${options.path || '/'}`;
    const method = options.method || 'GET';
    
    console.log(`[OTEL HTTP] Monkey-patching ${method} ${url}`);
    
    // If we have trace headers, propagate them to outgoing requests
    if (global.currentTraceHeaders) {
        Object.assign(options.headers, global.currentTraceHeaders);
        console.log(`[OTEL] 🔄 Propagated trace headers:`, global.currentTraceHeaders);
        
        // Validate propagated traceparent format
        if (global.currentTraceHeaders.traceparent) {
            const parts = global.currentTraceHeaders.traceparent.split('-');
            if (parts.length === 4) {
                console.log('[OTEL] ✅ Valid traceparent format:', {
                    version: parts[0],
                    traceId: parts[1], 
                    spanId: parts[2],
                    flags: parts[3]
                });
            } else {
                console.log('[OTEL] ❌ Invalid traceparent format:', global.currentTraceHeaders.traceparent);
            }
        }
        
        // Check if Beyla should detect this request
        validateBeylaDetection(options.headers, method, url);
    }
    
    return options;
}

// Validate if Beyla should detect our traces
function validateBeylaDetection(headers, method, url) {
    console.log('[BEYLA VALIDATION] 🔍 Checking if Beyla should detect this request:', {
        method,
        url,
        hasTraceparent: !!headers.traceparent,
        hasTracestate: !!headers.tracestate,
        userAgent: headers['user-agent'],
        contentType: headers['content-type']
    });

    // Check if this looks like a request Beyla should instrument
    const shouldBeInstrumented = !url?.includes('/socket.io') && 
                               !url?.includes('/v1/traces') &&
                               method !== 'OPTIONS';

    console.log('[BEYLA VALIDATION]', shouldBeInstrumented ? '✅' : '⚠️', 
        shouldBeInstrumented ? 'Beyla SHOULD instrument this' : 'Beyla might skip this request');

    return shouldBeInstrumented;
}

// Enhanced HTTP request monkey-patching
const originalHttpRequest = http.request;
http.request = function(options, callback) {
    const enhancedOptions = ensureTraceHeaders(options);
    
    // Log detailed request info
    console.log(`[OTEL HTTP] Injected trace context:`, enhancedOptions.headers);
    
    const req = originalHttpRequest.call(this, enhancedOptions, callback);
    
    // Special handling for OTLP traces endpoint
    const url = `${options.protocol || 'http:'}//${options.host || options.hostname}:${options.port || 80}${options.path || '/'}`;
    if (url.includes('/v1/traces')) {
        console.log('[OTEL EXPORT] 🚀 Detected trace export to OTLP endpoint');
        
        // Capture the request body to see what spans are being sent
        const originalWrite = req.write;
        const originalEnd = req.end;
        let body = '';
        
        req.write = function(chunk) {
            if (chunk) {
                body += chunk.toString();
            }
            return originalWrite.call(this, chunk);
        };
        
        req.end = function(chunk) {
            if (chunk) {
                body += chunk.toString();
            }
            
            if (body) {
                try {
                    const traceData = JSON.parse(body);
                    console.log('[OTEL EXPORT] 📊 Sending trace data:', {
                        resourceSpans: traceData.resourceSpans?.length || 0,
                        totalSpans: traceData.resourceSpans?.reduce((acc, rs) => 
                            acc + (rs.scopeSpans?.reduce((acc2, ss) => acc2 + (ss.spans?.length || 0), 0) || 0), 0
                        ) || 0,
                        firstSpans: traceData.resourceSpans?.[0]?.scopeSpans?.[0]?.spans?.slice(0, 3).map(span => ({
                            name: span.name,
                            traceId: span.traceId,
                            spanId: span.spanId,
                            parentSpanId: span.parentSpanId
                        })) || []
                    });
                } catch (e) {
                    console.log('[OTEL EXPORT] ❌ Failed to parse trace data:', e.message);
                    console.log('[OTEL EXPORT] Raw body length:', body.length);
                }
            }
            
            return originalEnd.call(this, chunk);
        };
        
        // Log response from OTLP collector
        req.on('response', (res) => {
            console.log('[OTEL EXPORT] 📨 OTLP collector response:', {
                statusCode: res.statusCode,
                statusMessage: res.statusMessage,
                headers: res.headers
            });
            
            let responseBody = '';
            res.on('data', (chunk) => {
                responseBody += chunk.toString();
            });
            
            res.on('end', () => {
                if (res.statusCode === 200) {
                    console.log('[OTEL EXPORT] ✅ Successfully sent traces to OTLP collector');
                } else {
                    console.log('[OTEL EXPORT] ❌ OTLP collector error:', {
                        statusCode: res.statusCode,
                        body: responseBody
                    });
                }
            });
        });
        
        req.on('error', (err) => {
            console.log('[OTEL EXPORT] ❌ Request to OTLP collector failed:', err.message);
        });
    }
    
    console.log(`[OTEL HTTP] Outgoing request: ${options.method || 'GET'} ${url}`);
    console.log(`[OTEL HTTP] Headers:`, req.getHeaders());
    
    return req;
};

// Monkey-patch https.request for header propagation
const originalHttpsRequest = https.request;
https.request = function(options, callback) {
    return originalHttpRequest.call(this, ensureTraceHeaders(options), callback);
};

console.log('[OTEL] ✅ Header propagation enabled (Beyla handles instrumentation)');

module.exports = { 
    propagation,
    
    // Create active span from incoming trace headers  
    setCurrentTraceHeaders: (headers) => {
        const traceHeaders = {};
        if (headers.traceparent) traceHeaders.traceparent = headers.traceparent;
        if (headers.tracestate) traceHeaders.tracestate = headers.tracestate;
        
        if (Object.keys(traceHeaders).length > 0) {
            global.currentTraceHeaders = traceHeaders;
            console.log('[OTEL] 📥 Captured trace headers for Beyla:', traceHeaders);
            
            // Manual span context creation from traceparent
            if (headers.traceparent) {
                try {
                    const parts = headers.traceparent.split('-');
                    if (parts.length === 4) {
                        const [version, traceId, spanId, flags] = parts;
                        
                        console.log('[OTEL] 🔍 Parsing traceparent:', {
                            version, traceId, spanId, flags,
                            traceIdLength: traceId.length,
                            spanIdLength: spanId.length
                        });
                        
                        // Validate trace and span ID lengths
                        if (traceId.length === 32 && spanId.length === 16) {
                            // Create span context using proper OpenTelemetry format
                            const remoteSpanContext = {
                                traceId: traceId,
                                spanId: spanId,
                                traceFlags: parseInt(flags, 16),
                                isRemote: true
                            };
                            
                            console.log('[OTEL] 🔍 Created remote span context:', {
                                ...remoteSpanContext,
                                isValid: trace.isSpanContextValid(remoteSpanContext)
                            });
                            
                            // Set the remote span context as active context
                            const remoteContext = trace.setSpanContext(context.active(), remoteSpanContext);
                            
                                        console.log('[OTEL] 🔍 Creating child span with tracer:', {
                tracerName: 'connector-cloudserver',
                hasProvider: !!trace.getTracerProvider(),
                parentTraceId: remoteSpanContext.traceId,
                remoteContextValid: trace.isSpanContextValid(remoteSpanContext)
            });
                            
                            // Execute span creation within the remote context to ensure inheritance
                            const { activeSpan, spanContext } = context.with(remoteContext, () => {
                                const span = tracer.startSpan('cloudserver-http-request', {
                                    kind: SpanKind.SERVER
                                    // No parent needed - should inherit from active context
                                });
                                return { activeSpan: span, spanContext: span.spanContext() };
                            });
                            
                            console.log('[OTEL] 🔍 Created child span context:', {
                                traceId: spanContext.traceId,
                                spanId: spanContext.spanId,
                                traceFlags: spanContext.traceFlags,
                                isValid: trace.isSpanContextValid(spanContext),
                                inheritedTrace: spanContext.traceId === remoteSpanContext.traceId
                            });
                            
                            // Set the span as active in global context
                            global.activeSpanContext = trace.setSpan(remoteContext, activeSpan);
                            global.activeSpan = activeSpan;
                            
                            console.log('[OTEL] ✅ Created active span from Beyla trace:', {
                                traceId: spanContext.traceId,
                                spanId: spanContext.spanId,
                                isActive: true,
                                isValid: trace.isSpanContextValid(spanContext)
                            });
                        } else {
                            console.log('[OTEL] ❌ Invalid trace/span ID lengths:', {
                                traceIdLength: traceId.length, 
                                spanIdLength: spanId.length,
                                expected: 'traceId=32, spanId=16'
                            });
                        }
                    }
                } catch (err) {
                    console.log('[OTEL] ❌ Failed to parse traceparent:', err.message);
                }
            }
        }
        
        return traceHeaders;
    },
    
    // Clear trace headers and active span context
    clearCurrentTraceHeaders: () => {
        if (global.activeSpan) {
            global.activeSpan.end();
        }
        global.currentTraceHeaders = null;
        global.activeSpanContext = null;
        global.activeSpan = null;
        console.log('[OTEL] 🧹 Cleared trace headers and active span context');
    }
}; 