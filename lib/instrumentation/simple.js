const { trace, context, SpanStatusCode, SpanKind } = require('@opentelemetry/api');

/**
 * Low-cardinality instrumentation using Proxy - instruments all methods
 * but with generic span names to avoid high cardinality issues
 */
function createInstrumentedProxy(target, componentName, options = {}) {
    if (!target || typeof target !== 'object') {
        return target;
    }

    const tracer = trace.getTracer(`cloudserver-${componentName}`, '1.0.0');
    const { 
        skipMethods = ['constructor', 'toString', 'valueOf', 'inspect'],
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
                        'cloudserver.method': prop,
                        // Keep cardinality low - no dynamic values
                        'cloudserver.operation_type': getOperationType(componentName, prop),
                        // Add some useful context without high cardinality
                        'cloudserver.method_name': prop
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
                const wrappedArgs = [...args];
                wrappedArgs[callbackIndex] = function (err, ...results) {
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
                return context.with(trace.setSpan(context.active(), span), () =>
                    originalValue.apply(this, wrappedArgs));
            };
        }
    });
}

/**
 * Get operation type for low cardinality grouping
 */
function getOperationType(component, method) {
    if (component === 'vault') {
        if (method.includes('authenticate')) {return 'auth';}
        if (method.includes('Policy') || method.includes('policy')) {return 'policy';}
        return 'other';
    }
    if (component === 'storage') {
        if (['put', 'upload'].some(op => method.includes(op))) {return 'write';}
        if (['get', 'head', 'download'].some(op => method.includes(op))) {return 'read';}
        if (['delete', 'remove'].some(op => method.includes(op))) {return 'delete';}
        return 'other';
    }
    if (component === 'metadata') {
        if (method.includes('Bucket')) {return 'bucket_op';}
        if (method.includes('Object')) {return 'object_op';}
        return 'other';
    }
    return 'unknown';
}

/**
 * Instrument vault operations with low cardinality span names
 */
function instrumentVault(vault) {
    return createInstrumentedProxy(vault, 'vault', {
        getSpanName: methodName => {
            // Use generic span names to avoid high cardinality
            if (methodName.includes('authenticate')) {
                return 'vault.authenticate';
            }
            if (methodName.includes('Policy') || methodName.includes('policy')) {
                return 'vault.policy_check';
            }
            return `vault.${methodName}`;
        }
    });
}

/**
 * Instrument storage operations with low cardinality span names
 */
function instrumentStorage(storage) {
    return createInstrumentedProxy(storage, 'storage', {
        getSpanName: methodName => {
            // Use generic span names to avoid high cardinality from object keys
            if (methodName === 'put' || methodName.includes('upload')) {
                return 'storage.put';
            }
            if (methodName === 'get' || methodName.includes('download')) {
                return 'storage.get';
            }
            if (methodName === 'delete' || methodName.includes('remove')) {
                return 'storage.delete';
            }
            if (methodName === 'head') {
                return 'storage.head';
            }
            return `storage.${methodName}`;
        }
    });
}

/**
 * Instrument metadata operations with low cardinality span names
 */
function instrumentMetadata(metadata) {
    return createInstrumentedProxy(metadata, 'metadata', {
        getSpanName: methodName => {
            // Use generic span names to avoid high cardinality from bucket/object names
            if (methodName.includes('Object')) {
                if (methodName.includes('put') || methodName.includes('create')) {
                    return 'metadata.object_put';
                }
                if (methodName.includes('get') || methodName.includes('read')) {
                    return 'metadata.object_get';
                }
                if (methodName.includes('delete') || methodName.includes('remove')) {
                    return 'metadata.object_delete';
                }
                return 'metadata.object_op';
            }
            if (methodName.includes('Bucket')) {
                if (methodName.includes('create')) {
                    return 'metadata.bucket_create';
                }
                if (methodName.includes('delete')) {
                    return 'metadata.bucket_delete';
                }
                if (methodName.includes('get') || methodName.includes('Attributes')) {
                    return 'metadata.bucket_get';
                }
                if (methodName.includes('put') || methodName.includes('update')) {
                    return 'metadata.bucket_put';
                }
                return 'metadata.bucket_op';
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
