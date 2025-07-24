const { trace, context } = require('@opentelemetry/api');
const { SemanticAttributes } = require('@opentelemetry/semantic-conventions');

/**
 * MongoDB instrumentation that creates spans when Beyla is likely active
 * Since Beyla uses eBPF instrumentation, we can't detect active spans via OpenTelemetry API
 * Instead, we create MongoDB spans and let them connect via OTLP export
 */
function instrumentArsenalMongoDB(metadataWrapper) {
    if (!metadataWrapper || typeof metadataWrapper !== 'object') {
        return metadataWrapper;
    }

    const methodsToInstrument = [
        'putObjectMD',
        'getObjectMD', 
        'getBucketAndObjectMD',
        'deleteObjectMD',
        'listObject',
        'putBucketAttributes',
        'getBucketAttributes',
        'deleteBucket',
        'createBucket',
        'listBuckets'
    ];

    methodsToInstrument.forEach(methodName => {
        if (typeof metadataWrapper[methodName] === 'function') {
            const originalMethod = metadataWrapper[methodName].bind(metadataWrapper);
            
            metadataWrapper[methodName] = function(...args) {
                // Extract operation details with generic naming for low cardinality
                const bucketName = args[0] || 'unknown-bucket';
                const spanName = `mongodb.${methodName} ${bucketName}/*`;

                // Get tracer and create span - should automatically inherit Beyla trace ID
                const tracer = trace.getTracer('connector-cloudserver-mongodb', '1.0.0');
                const span = tracer.startSpan(spanName, {
                    kind: 1, // INTERNAL
                    attributes: {
                        [SemanticAttributes.DB_SYSTEM]: 'mongodb',
                        [SemanticAttributes.DB_OPERATION]: methodName,
                        [SemanticAttributes.DB_NAME]: 'metadata',
                        'db.mongodb.collection_name': bucketName,
                        'arsenal.method': methodName,
                        'arsenal.bucket_name': bucketName
                        // Note: Using generic bucket/* pattern for low cardinality
                    }
                }); // Let OpenTelemetry automatically use active context

                // Find callback in arguments (usually last argument)
                const callbackIndex = args.findIndex(arg => typeof arg === 'function');
                if (callbackIndex === -1) {
                    // No callback found, call original method and end span
                    span.setStatus({ code: 1 }); // OK
                    span.end();
                    return originalMethod.apply(this, args);
                }

                // Wrap callback to end span when operation completes
                const originalCallback = args[callbackIndex];
                args[callbackIndex] = function(err, result) {
                    if (err) {
                        span.recordException(err);
                        span.setStatus({ code: 2, message: err.message }); // ERROR
                    } else {
                        span.setStatus({ code: 1 }); // OK
                    }
                    span.end();
                    return originalCallback.call(this, err, result);
                };

                // Execute original method within span context
                return context.with(trace.setSpan(context.active(), span), () => {
                    return originalMethod.apply(this, args);
                });
            };
        }
    });
    
    return metadataWrapper;
}

module.exports = {
    instrumentArsenalMongoDB
}; 