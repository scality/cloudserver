const { propagation } = require('@opentelemetry/api');

// Simple trace header propagation - let Beyla handle all instrumentation 
console.log('[OTEL] Initializing header propagation (Beyla handles spans)...');

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
    
    // Simple header capture for Beyla instrumentation
    setCurrentTraceHeaders: (headers) => {
        const traceHeaders = {};
        if (headers.traceparent) traceHeaders.traceparent = headers.traceparent;
        if (headers.tracestate) traceHeaders.tracestate = headers.tracestate;
        
        if (Object.keys(traceHeaders).length > 0) {
            global.currentTraceHeaders = traceHeaders;
            console.log('[OTEL] 📥 Captured trace headers for Beyla:', traceHeaders);
        }
        
        return traceHeaders;
    },
    
    // Clear trace headers
    clearCurrentTraceHeaders: () => {
        global.currentTraceHeaders = null;
        console.log('[OTEL] 🧹 Cleared trace headers');
    }
}; 