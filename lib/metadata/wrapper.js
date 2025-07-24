const MetadataWrapper = require('arsenal').storage.metadata.MetadataWrapper;
const { config } = require('../Config');
const logger = require('../utilities/logger');
const constants = require('../../constants');
const bucketclient = require('bucketclient');
const { instrumentArsenalMongoDB } = require('../instrumentation/mongoArsenal');

const clientName = config.backends.metadata;

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

let metadata = new MetadataWrapper(config.backends.metadata, params,
    bucketclient, logger);

if (clientName === 'mongodb') {
    metadata = instrumentArsenalMongoDB(metadata);
}

const originalSetup = metadata.setup;
if (originalSetup) {
    metadata.setup = function (callback) {
        return originalSetup.call(this, (err, result) => callback(err, result));
    };
}

if (clientName === 'mongodb') {
    const methodsToLog = [
        'putObjectMD', 'getObjectMD', 'deleteObjectMD', 'listObject',
        'putBucketAttributes', 'getBucketAttributes', 'deleteBucket', 'createBucket',
        'getBucket', 'getBucketAndObjectMD',
    ];

    methodsToLog.forEach(methodName => {
        if (typeof metadata[methodName] === 'function') {
            const originalMethod = metadata[methodName].bind(metadata);
            
            metadata[methodName] = function (...args) {
                const callbackIndex = args.findIndex(arg => typeof arg === 'function');
                if (callbackIndex !== -1) {
                    const originalCallback = args[callbackIndex];
                    // eslint-disable-next-line no-param-reassign
                    args[callbackIndex] = function (err, result) {
                        return originalCallback.call(this, err, result);
                    };
                }

                return originalMethod.apply(this, args);
            };
        }
    });
}

module.exports = metadata;
