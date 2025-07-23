const { trace, context } = require('@opentelemetry/api');
const { SemanticAttributes } = require('@opentelemetry/semantic-conventions');

const tracer = trace.getTracer('arsenal-mongodb-instrumentation', '1.0.0');

/**
 * Wrap Arsenal's MetadataWrapper MongoDB methods to create manual spans
 */
function instrumentArsenalMongoDB(metadataWrapper) {
    if (!metadataWrapper || typeof metadataWrapper !== 'object') {
        console.log('[MONGO-ARSENAL] No metadata wrapper to instrument');
        return metadataWrapper;
    }

    // Common methods to instrument
    const methodsToInstrument = [
        'putObjectMD',
        'getObjectMD', 
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
                // Extract operation details
                const bucketName = args[0] || 'unknown-bucket';
                const objectKey = args[1] || null;
                const operation = objectKey ? 
                    `${methodName}(${bucketName}/${objectKey})` : 
                    `${methodName}(${bucketName})`;
                
                const spanName = `mongodb.${methodName}`;
                console.log(`[MONGO-ARSENAL] 🔥 ${operation} operation on ${bucketName}${objectKey ? '/' + objectKey : ''}`);
                
                // Create span
                const span = tracer.startSpan(spanName, {
                    kind: 1, // INTERNAL
                    attributes: {
                        [SemanticAttributes.DB_SYSTEM]: 'mongodb',
                        [SemanticAttributes.DB_OPERATION]: methodName,
                        [SemanticAttributes.DB_NAME]: 'metadata',
                        'arsenal.bucket.name': bucketName,
                        ...(objectKey && { 'arsenal.object.key': objectKey })
                    }
                });

                console.log(`[MONGO-ARSENAL] 📍 Span ID: ${span.spanContext().spanId}`);
                console.log(`[MONGO-ARSENAL] 📍 Trace ID: ${span.spanContext().traceId}`);

                // Find callback and wrap it
                const callbackIndex = args.findIndex(arg => typeof arg === 'function');
                if (callbackIndex !== -1) {
                    const originalCallback = args[callbackIndex];
                    args[callbackIndex] = function(err, result) {
                        if (err) {
                            span.recordException(err);
                            span.setStatus({ code: 2, message: err.message });
                            console.log(`[MONGO-ARSENAL] ❌ ${operation} failed`);
                        } else {
                            span.setStatus({ code: 1 });
                            console.log(`[MONGO-ARSENAL] ✅ ${operation} succeeded`);
                        }
                        
                        span.end();
                        return originalCallback.call(this, err, result);
                    };
                }

                return originalMethod.apply(this, args);
            };
        }
    });

    console.log(`[MONGO-ARSENAL] 📝 Instrumented ${methodsToInstrument.length} MongoDB methods`);
    return metadataWrapper;
}

module.exports = { instrumentArsenalMongoDB }; 