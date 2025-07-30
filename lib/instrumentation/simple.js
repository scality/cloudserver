const { trace, context, SpanStatusCode, SpanKind } = require('@opentelemetry/api');

const enableOtel = process.env.ENABLE_OTEL === 'true';


/**
 * Low-cardinality instrumentation using Proxy - instruments all methods
 * but with generic span names to avoid high cardinality issues
 */
function createInstrumentedProxy(target, componentName, options = {}) {
    if (!enableOtel) {
        return target;
    }

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
        if (method.includes('Object')) {
            if (method.includes('put') || method.includes('create')) {return 'object_put';}
            if (method.includes('get') || method.includes('read')) {return 'object_get';}
            if (method.includes('delete') || method.includes('remove')) {return 'object_delete';}
            return 'object_op';
        }
        if (method.includes('Bucket')) {
            if (method.includes('create')) {return 'bucket_create';}
            if (method.includes('delete')) {return 'bucket_delete';}
            if (method.includes('get') || method.includes('Attributes')) {return 'bucket_get';}
            if (method.includes('put') || method.includes('update')) {return 'bucket_put';}
            return 'bucket_op';
        }
        return 'other';
    }
    if (component === 'api') {
        if (method.includes('object')) {
            if (method.includes('Put') || method.includes('put')) {return 'object_put';}
            if (method.includes('Get') || method.includes('get') || 
                method.includes('Head') || method.includes('head')) {return 'object_get';}
            if (method.includes('Delete') || method.includes('delete')) {return 'object_delete';}
            if (method.includes('Copy') || method.includes('copy')) {return 'object_copy';}
            return 'object_op';
        }
        if (method.includes('bucket')) {
            if (method.includes('Put') || method.includes('put')) {return 'bucket_put';}
            if (method.includes('Get') || method.includes('get') || 
                method.includes('Head') || method.includes('head')) {return 'bucket_get';}
            if (method.includes('Delete') || method.includes('delete')) {return 'bucket_delete';}
            return 'bucket_op';
        }
        if (method.includes('multipart') || method.includes('Multipart')) {return 'multipart';}
        return 'other';
    }
    if (component === 'services') {
        if (method.includes('Object')) {
            if (method.includes('Store') || method.includes('store')) {return 'object_store';}
            if (method.includes('Delete') || method.includes('delete')) {return 'object_delete';}
            if (method.includes('Get') || method.includes('get') || method.includes('Listing')) {return 'object_get';}
            return 'object_op';
        }
        if (method.includes('Multipart') || method.includes('MPU')) {return 'multipart';}
        if (method.includes('Service')) {return 'service_listing';}
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

/**
 * Instrument API layer operations with detailed breakdown
 */
function instrumentApi(api) {
    return createInstrumentedProxy(api, 'api', {
        getSpanName: methodName => {
            // Create detailed but low-cardinality span names for API operations
            if (methodName === 'callApiMethod') {
                return 'api.request_handler';
            }

            // Object operations
            if (methodName.startsWith('object')) {
                if (methodName.includes('Put') || methodName === 'objectPut') {
                    return 'api.object_put';
                }
                if (methodName.includes('Get') || methodName === 'objectGet') {
                    return 'api.object_get';
                }
                if (methodName.includes('Head') || methodName === 'objectHead') {
                    return 'api.object_head';
                }
                if (methodName.includes('Delete') || methodName === 'objectDelete') {
                    return 'api.object_delete';
                }
                if (methodName.includes('Copy') || methodName === 'objectCopy') {
                    return 'api.object_copy';
                }
                if (methodName.includes('ACL')) {
                    return 'api.object_acl';
                }
                if (methodName.includes('Tag')) {
                    return 'api.object_tagging';
                }
                return 'api.object_op';
            }

            // Bucket operations
            if (methodName.startsWith('bucket')) {
                if (methodName.includes('Put') || methodName === 'bucketPut') {
                    return 'api.bucket_put';
                }
                if (methodName.includes('Get') || methodName === 'bucketGet') {
                    return 'api.bucket_get';
                }
                if (methodName.includes('Head') || methodName === 'bucketHead') {
                    return 'api.bucket_head';
                }
                if (methodName.includes('Delete') || methodName === 'bucketDelete') {
                    return 'api.bucket_delete';
                }
                if (methodName.includes('ACL')) {
                    return 'api.bucket_acl';
                }
                if (methodName.includes('Policy')) {
                    return 'api.bucket_policy';
                }
                if (methodName.includes('Cors')) {
                    return 'api.bucket_cors';
                }
                if (methodName.includes('Lifecycle')) {
                    return 'api.bucket_lifecycle';
                }
                if (methodName.includes('Versioning')) {
                    return 'api.bucket_versioning';
                }
                if (methodName.includes('Tag')) {
                    return 'api.bucket_tagging';
                }
                return 'api.bucket_op';
            }

            // Multipart operations
            if (methodName.includes('multipart') || methodName.includes('Multipart')) {
                if (methodName.includes('initiate') || methodName.includes('Initiate')) {
                    return 'api.multipart_initiate';
                }
                if (methodName.includes('complete') || methodName.includes('Complete')) {
                    return 'api.multipart_complete';
                }
                if (methodName.includes('Part')) {
                    return 'api.multipart_upload_part';
                }
                if (methodName.includes('list') || methodName.includes('List')) {
                    return 'api.multipart_list';
                }
                return 'api.multipart_op';
            }

            // Service-level operations
            if (methodName.includes('service') || methodName.includes('Service')) {
                return 'api.service_get';
            }

            // Website operations
            if (methodName.includes('website')) {
                return 'api.website';
            }

            // CORS preflight
            if (methodName.includes('cors') || methodName.includes('Cors')) {
                return 'api.cors_preflight';
            }

            return `api.${methodName}`;
        }
    });
}

/**
 * Instrument services layer operations with business logic breakdown
 */
function instrumentServices(services) {
    return createInstrumentedProxy(services, 'services', {
        getSpanName: methodName => {
            // Create meaningful span names for business logic operations
            if (methodName === 'metadataStoreObject') {
                return 'services.store_object';
            }
            if (methodName === 'deleteObject') {
                return 'services.delete_object';
            }
            if (methodName === 'getObjectListing') {
                return 'services.list_objects';
            }
            if (methodName === 'getLifecycleListing') {
                return 'services.lifecycle_listing';
            }
            if (methodName === 'getService') {
                return 'services.list_buckets';
            }
            if (methodName.includes('MPU') || methodName.includes('Multipart')) {
                if (methodName.includes('Store')) {
                    return 'services.multipart_store';
                }
                if (methodName.includes('Validate')) {
                    return 'services.multipart_validate';
                }
                if (methodName.includes('Mark')) {
                    return 'services.multipart_mark_complete';
                }
                if (methodName.includes('Listing')) {
                    return 'services.multipart_list';
                }
                if (methodName.includes('Part')) {
                    return 'services.multipart_store_part';
                }
                return 'services.multipart_op';
            }
            if (methodName.includes('batchDelete')) {
                return 'services.batch_delete';
            }
            return `services.${methodName}`;
        }
    });
}

/**
 * Create a span wrapper for request processing functions
 */
function createRequestSpan(spanName, fn, tracer, attributes = {}) {
    if (!enableOtel) {
        return fn;
    }

    return function (...args) {
        const span = tracer.startSpan(spanName, {
            kind: SpanKind.INTERNAL,
            attributes: {
                'cloudserver.component': 'request_processing',
                ...attributes
            }
        });

        // Find callback in arguments (usually last function argument)
        const callbackIndex = args.findIndex(arg => typeof arg === 'function');

        if (callbackIndex === -1) {
            // No callback - handle synchronous call
            try {
                const result = fn.apply(this, args);
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

        // Callback-based - wrap the callback
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
            fn.apply(this, wrappedArgs));
    };
}

/**
 * Instrument a specific API method with detailed span creation
 */
function instrumentApiMethod(apiMethod, methodName) {
    if (!enableOtel) {
        return apiMethod;
    }

    const tracer = trace.getTracer('cloudserver-api-detailed', '1.0.0');

    return function (...args) {
        // Extract request info for better observability (keeping low cardinality)
        const request = args.find(arg => arg && arg.method && arg.url);
        const attributes = {
            'cloudserver.component': 'api',
            'cloudserver.operation_type': getOperationType('api', methodName),
            'cloudserver.method_name': methodName
        };

        if (request) {
            attributes['http.method'] = request.method;
            // Don't include full URL to avoid high cardinality
            attributes['cloudserver.api_operation'] = methodName;
        }

        const span = tracer.startSpan(`api.${getGenericApiSpanName(methodName)}`, {
            kind: SpanKind.INTERNAL,
            attributes
        });

        // Find callback in arguments
        const callbackIndex = args.findIndex(arg => typeof arg === 'function');

        if (callbackIndex === -1) {
            // No callback found - shouldn't happen in CloudServer but handle gracefully
            try {
                const result = apiMethod.apply(this, args);
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

        // Wrap the callback
        const originalCallback = args[callbackIndex];
        const wrappedArgs = [...args];
        wrappedArgs[callbackIndex] = function (err, ...results) {
            if (err) {
                span.recordException(err);
                span.setStatus({ code: SpanStatusCode.ERROR });
                // Add error type as attribute for better debugging
                if (err.code) {
                    span.setAttributes({ 'cloudserver.error_code': err.code });
                }
            } else {
                span.setStatus({ code: SpanStatusCode.OK });
            }
            span.end();
            return originalCallback.call(this, err, ...results);
        };

        // Execute within span context
        return context.with(trace.setSpan(context.active(), span), () =>
            apiMethod.apply(this, wrappedArgs));
    };
}

/**
 * Get generic API span name to avoid high cardinality
 */
function getGenericApiSpanName(methodName) {
    if (!enableOtel) {
        return methodName;
    }

    // Convert specific method names to generic categories
    if (methodName.includes('object')) {
        if (methodName.includes('Put')) {return 'object_put';}
        if (methodName.includes('Get')) {return 'object_get';}
        if (methodName.includes('Head')) {return 'object_head';}
        if (methodName.includes('Delete')) {return 'object_delete';}
        if (methodName.includes('Copy')) {return 'object_copy';}
        return 'object_op';
    }
    if (methodName.includes('bucket')) {
        if (methodName.includes('Put')) {return 'bucket_put';}
        if (methodName.includes('Get')) {return 'bucket_get';}
        if (methodName.includes('Head')) {return 'bucket_head';}
        if (methodName.includes('Delete')) {return 'bucket_delete';}
        return 'bucket_op';
    }
    if (methodName.includes('multipart') || methodName.includes('Multipart')) {
        return 'multipart_op';
    }
    return methodName.toLowerCase();
}

module.exports = {
    createInstrumentedProxy,
    instrumentVault,
    instrumentStorage,
    instrumentMetadata,
    instrumentApi,
    instrumentServices,
    createRequestSpan,
    instrumentApiMethod
}; 
