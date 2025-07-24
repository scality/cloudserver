const { trace, context, SpanStatusCode, SpanKind } = require('@opentelemetry/api');

/**
 * Simple, non-invasive instrumentation using Proxy
 * Automatically instruments any method call on the target object
 */
function createInstrumentedProxy(target, componentName, options = {}) {
    if (!target || typeof target !== 'object') {
        return target;
    }

    const tracer = trace.getTracer(`cloudserver-${componentName}`, '1.0.0');
    const { 
        skipMethods = ['constructor', 'toString', 'valueOf'],
        getSpanName = methodName => `${componentName}.${methodName}`
    } = options;

    return new Proxy(target, {
        get(obj, prop) {
            const originalValue = obj[prop];
            
            // Skip if not a function or if it's in the skip list
            if (typeof originalValue !== 'function' || skipMethods.includes(prop)) {
                return originalValue;
            }

            // Return instrumented version of the function
            return function (...args) {
                const spanName = getSpanName(prop, args);
                const span = tracer.startSpan(spanName, {
                    kind: SpanKind.INTERNAL,
                    attributes: {
                        'cloudserver.component': componentName,
                        'cloudserver.method': prop
                    }
                });

                // Find callback in arguments (usually last function argument)
                const callbackIndex = args.findIndex(arg => typeof arg === 'function');
                
                if (callbackIndex === -1) {
                    // No callback - handle synchronous or promise-based method
                    try {
                        const result = originalValue.apply(this, args);
                        
                        // Handle promises
                        if (result && typeof result.then === 'function') {
                            return result
                                .then(value => {
                                    span.setStatus({ code: SpanStatusCode.OK });
                                    span.end();
                                    return value;
                                })
                                .catch(error => {
                                    span.recordException(error);
                                    span.setStatus({ code: SpanStatusCode.ERROR });
                                    span.end();
                                    throw error;
                                });
                        }
                        
                        // Synchronous method
                        span.setStatus({ code: SpanStatusCode.OK });
                        span.end();
                        return result;
                    } catch (error) {
                        span.recordException(error);
                        span.setStatus({ code: SpanStatusCode.ERROR });
                        span.end();
                        throw error;
                    }
                }

                // Callback-based method - wrap the callback
                const originalCallback = args[callbackIndex];
                // eslint-disable-next-line no-param-reassign
                args[callbackIndex] = function (err, ...results) {
                    if (err) {
                        span.recordException(err);
                        span.setStatus({ code: SpanStatusCode.ERROR });
                    } else {
                        span.setStatus({ code: SpanStatusCode.OK });
                    }
                    span.end();
                    return originalCallback.call(this, err, ...results);
                };

                // Execute within span context
                return context.with(trace.setSpan(context.active(), span), () => originalValue.apply(this, args));
            };
        }
    });
}

/**
 * Instrument vault operations
 */
function instrumentVault(vault) {
    return createInstrumentedProxy(vault, 'vault', {
        getSpanName: (methodName, args) => {
            // Create more meaningful span names for vault operations
            if (methodName.includes('authenticate')) {
                const accessKey = args[0]?.data?.accessKey || 'unknown';
                return `vault.auth ${accessKey}`;
            }
            return `vault.${methodName}`;
        }
    });
}

/**
 * Instrument storage operations  
 */
function instrumentStorage(storage) {
    return createInstrumentedProxy(storage, 'storage', {
        getSpanName: (methodName, args) => {
            // Create more meaningful span names for storage operations
            if (methodName === 'put') {
                const objectKey = args[2]?.objectKey || 'unknown';
                return `storage.put ${objectKey}`;
            }
            if (methodName === 'get') {
                const objectKey = args[0]?.objectKey || args[0]?.key || 'unknown';
                return `storage.get ${objectKey}`;
            }
            if (methodName === 'delete') {
                const objectKey = args[0]?.objectKey || args[0]?.key || 'unknown';
                return `storage.delete ${objectKey}`;
            }
            return `storage.${methodName}`;
        }
    });
}

/**
 * Instrument metadata operations
 */
function instrumentMetadata(metadata) {
    return createInstrumentedProxy(metadata, 'metadata', {
        getSpanName: (methodName, args) => {
            // Create more meaningful span names for metadata operations
            if (methodName.includes('Object')) {
                const bucketName = args[0] || 'unknown';
                const objectKey = args[1] || 'unknown';
                return `metadata.${methodName} ${bucketName}/${objectKey}`;
            }
            if (methodName.includes('Bucket')) {
                const bucketName = args[0] || 'unknown';
                return `metadata.${methodName} ${bucketName}`;
            }
            return `metadata.${methodName}`;
        }
    });
}

module.exports = {
    createInstrumentedProxy,
    instrumentVault,
    instrumentStorage,
    instrumentMetadata
}; 
