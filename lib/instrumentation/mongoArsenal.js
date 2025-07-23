const { trace, context } = require('@opentelemetry/api');
const { SemanticAttributes } = require('@opentelemetry/semantic-conventions');

const tracer = trace.getTracer('connector-cloudserver', '1.0.0');

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
                // Extract operation details
                const bucketName = args[0] || 'unknown-bucket';
                const objectKey = args[1] || null;
                const spanName = objectKey ? 
                    `mongodb.${methodName} ${bucketName}/${objectKey}` : 
                    `mongodb.${methodName} ${bucketName}`;

                // Use global active span context from Beyla headers
                const activeContext = global.activeSpanContext || context.active();
                const activeSpan = global.activeSpan || trace.getActiveSpan(activeContext);
                
                // If we have a global span, make sure we use the context that contains it
                const contextWithSpan = activeSpan ? trace.setSpan(activeContext, activeSpan) : activeContext;
                
                console.log(`[MONGO-ARSENAL] 🔍 Active context:`, {
                    isEmpty: activeContext === context.ROOT_CONTEXT,
                    contextKeys: Object.keys(activeContext || {}),
                    activeSpan: activeSpan ? 'present' : 'missing',
                    globalContext: global.activeSpanContext ? 'available' : 'missing',
                    globalSpan: global.activeSpan ? 'available' : 'missing'
                });
                
                if (activeSpan) {
                    const parentSpanContext = activeSpan.spanContext();
                    console.log(`[MONGO-ARSENAL] 🔍 Parent span context:`, {
                        traceId: parentSpanContext.traceId,
                        spanId: parentSpanContext.spanId,
                        isValid: trace.isSpanContextValid(parentSpanContext)
                    });
                }
                
                // Create span within the active context to ensure proper trace inheritance
                const span = context.with(contextWithSpan, () => {
                    return tracer.startSpan(spanName, {
                        kind: 1, // INTERNAL
                        attributes: {
                            [SemanticAttributes.DB_SYSTEM]: 'mongodb',
                            [SemanticAttributes.DB_OPERATION]: methodName,
                            [SemanticAttributes.DB_NAME]: 'metadata',
                            'db.mongodb.collection_name': bucketName,
                            'arsenal.method': methodName,
                            'arsenal.bucket_name': bucketName,
                            ...(objectKey && { 'arsenal.object_key': objectKey })
                        }
                    });
                });

                console.log(`[MONGO-ARSENAL] 🔥 ${methodName} operation on ${bucketName}${objectKey ? '/' + objectKey : ''}`);
                console.log(`[MONGO-ARSENAL] 📍 Span ID: ${span.spanContext().spanId}`);
                console.log(`[MONGO-ARSENAL] 📍 Trace ID: ${span.spanContext().traceId}`);
                
                // Check if trace inheritance worked
                const expectedTraceId = activeSpan ? activeSpan.spanContext().traceId : 'none';
                const actualTraceId = span.spanContext().traceId;
                console.log(`[MONGO-ARSENAL] 🔍 Trace inheritance: expected=${expectedTraceId}, actual=${actualTraceId}, inherited=${expectedTraceId === actualTraceId}`);
                
                // Ensure span is ended even if no callback
                let spanEnded = false;
                const ensureSpanEnded = () => {
                    if (!spanEnded) {
                        spanEnded = true;
                        span.end();
                        console.log(`[MONGO-ARSENAL] 🏁 ${methodName} span ended and should be exported`);
                    }
                };

                // Add timeout to ensure span is ended
                const spanTimeout = setTimeout(() => {
                    console.log(`[MONGO-ARSENAL] ⏱️ ${methodName} span timeout, ending span`);
                    ensureSpanEnded();
                }, 10000); // 10 second timeout

                // Find callback in arguments (usually last argument)
                const callbackIndex = args.findIndex(arg => typeof arg === 'function');
                if (callbackIndex === -1) {
                    // No callback found, call original method
                    clearTimeout(spanTimeout);
                    ensureSpanEnded();
                    return originalMethod.apply(this, args);
                }

                // Wrap callback to end span
                const originalCallback = args[callbackIndex];
                args[callbackIndex] = function(err, result) {
                    clearTimeout(spanTimeout);
                    if (err) {
                        span.recordException(err);
                        span.setStatus({ code: 2, message: err.message }); // ERROR
                        console.log(`[MONGO-ARSENAL] ❌ ${methodName} failed:`, err.message);
                    } else {
                        span.setStatus({ code: 1 }); // OK
                        console.log(`[MONGO-ARSENAL] ✅ ${methodName} succeeded`);
                    }
                    ensureSpanEnded();
                    return originalCallback.call(this, err, result);
                };

                // Call original method in active span context  
                const spanContext = trace.setSpan(contextWithSpan, span);
                return context.with(spanContext, () => {
                    return originalMethod.apply(this, args);
                });
            };
        }
    });

    console.log('[MONGO-ARSENAL] ✅ Instrumented Arsenal MongoDB methods:', methodsToInstrument.filter(m => typeof metadataWrapper[m] === 'function'));
    return metadataWrapper;
}

module.exports = {
    instrumentArsenalMongoDB
}; 