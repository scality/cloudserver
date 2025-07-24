// Simple trace header propagation for Beyla compatibility
// No full OpenTelemetry context - just pass headers through

function ensureTraceContextInHeaders(headers) {
    if (!headers) {
        // eslint-disable-next-line no-param-reassign
        headers = {};
    }
    
    // If we have stored trace headers, propagate them
    if (global.currentTraceHeaders) {
        Object.assign(headers, global.currentTraceHeaders);
    }
    
    return headers;
}

function withTraceContext(callback) {
    return (...args) => {
        // Extract trace headers from incoming request if available
        const headers = args[0]?.headers || {};
        const traceHeaders = {};
        
        // Capture OpenTelemetry trace headers
        if (headers.traceparent) {
            traceHeaders.traceparent = headers.traceparent;
        }
        if (headers.tracestate) {
            traceHeaders.tracestate = headers.tracestate;
        }
        
        // Store for outgoing requests
        if (Object.keys(traceHeaders).length > 0) {
            global.currentTraceHeaders = traceHeaders;
        }
        
        return callback(...args);
    };
}

function wrapHttpProxyForContext(proxy) {
    if (!proxy?.web) {
        return proxy;
    }
    
    // Store original web method
    const originalWeb = proxy.web.bind(proxy);
    
    // Override the web method to inject trace headers
    // eslint-disable-next-line no-param-reassign
    proxy.web = (req, res, options, callback) => {
        // Inject stored trace headers into outgoing proxy requests
        if (req && req.headers) {
            ensureTraceContextInHeaders(req.headers);
        }
        
        // Call original web method
        return originalWeb(req, res, options, callback);
    };
    
    return proxy;
}

module.exports = {
    ensureTraceContextInHeaders,
    withTraceContext,
    wrapHttpProxyForContext,
};
