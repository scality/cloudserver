const MetadataWrapper = require('arsenal').storage.metadata.MetadataWrapper;
const { config } = require('../Config');
const logger = require('../utilities/logger');
const constants = require('../../constants');
const bucketclient = require('bucketclient');
const { instrumentArsenalMongoDB } = require('../instrumentation/mongoArsenal');

const clientName = config.backends.metadata;
console.log('[METADATA] Backend type:', clientName);

let params;
if (clientName === 'mem') {
    params = {};
} else if (clientName === 'file') {
    params = {
        metadataClient: {
            host: config.metadataClient.host,
            port: config.metadataClient.port,
        },
        constants: {
            usersBucket: constants.usersBucket,
            splitter: constants.splitter,
        },
        noDbOpen: null,
    };
} else if (clientName === 'scality') {
    params = {
        bucketdBootstrap: config.bucketd.bootstrap,
        bucketdLog: config.bucketd.log,
        https: config.https,
    };
} else if (clientName === 'mongodb') {
    console.log('[METADATA] MongoDB config:', JSON.stringify(config.mongodb, null, 2));
    params = {
        mongodb: config.mongodb,
        replicationGroupId: config.replicationGroupId,
        config,
    };
} else if (clientName === 'cdmi') {
    params = {
        cdmi: config.cdmi,
    };
}

console.log('[METADATA] Creating MetadataWrapper with params:', JSON.stringify(params, null, 2));

let metadata = new MetadataWrapper(config.backends.metadata, params,
    bucketclient, logger);

// Apply MongoDB instrumentation for trace propagation
if (clientName === 'mongodb') {
    console.log('[METADATA] 🔧 Applying MongoDB OpenTelemetry instrumentation...');
    metadata = instrumentArsenalMongoDB(metadata);
}

// Add debug logging for setup method
const originalSetup = metadata.setup;
if (originalSetup) {
    metadata.setup = function(callback) {
        console.log('[METADATA] 🔧 Starting MetadataWrapper setup...');
        return originalSetup.call(this, (err, result) => {
            if (err) {
                console.log('[METADATA] ❌ Setup failed:', err.message);
                console.log('[METADATA] ❌ Full error:', err);
            } else {
                console.log('[METADATA] ✅ Setup completed successfully');
            }
            return callback(err, result);
        });
    };
} else {
    console.log('[METADATA] ⚠️ No setup method found on MetadataWrapper');
}

// Add lightweight logging for key MongoDB operations (no full instrumentation)
if (clientName === 'mongodb') {
    const methodsToLog = [
        'putObjectMD', 'getObjectMD', 'deleteObjectMD', 'listObject',
        'putBucketAttributes', 'getBucketAttributes', 'deleteBucket', 'createBucket',
        'getBucket', 'getBucketAndObjectMD'  // Add these missing methods
    ];

    methodsToLog.forEach(methodName => {
        if (typeof metadata[methodName] === 'function') {
            const originalMethod = metadata[methodName].bind(metadata);
            
            metadata[methodName] = function(...args) {
                // Extract operation details for logging
                const bucketName = args[0] || 'unknown-bucket';
                const objectKey = args[1] || null;
                const operation = objectKey ? 
                    `${methodName}(${bucketName}/${objectKey})` : 
                    `${methodName}(${bucketName})`;

                console.log(`[MONGODB] 🔥 ${operation}`);
                
                // Find callback and wrap it to log completion
                const callbackIndex = args.findIndex(arg => typeof arg === 'function');
                if (callbackIndex !== -1) {
                    const originalCallback = args[callbackIndex];
                    args[callbackIndex] = function(err, result) {
                        if (err) {
                            console.log(`[MONGODB] ❌ ${operation} failed:`, err.message);
                        } else {
                            console.log(`[MONGODB] ✅ ${operation} succeeded`);
                        }
                        return originalCallback.call(this, err, result);
                    };
                }

                return originalMethod.apply(this, args);
            };
        } else {
            console.log(`[MONGODB] ⚠️  Method ${methodName} not found on metadata object`);
        }
    });

    console.log('[MONGODB] 📝 Added comprehensive logging for MongoDB operations');
    console.log('[MONGODB] Available methods:', Object.getOwnPropertyNames(metadata).filter(name => typeof metadata[name] === 'function'));
}

module.exports = metadata;
