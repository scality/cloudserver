const MetadataWrapper = require('arsenal').storage.metadata.MetadataWrapper;
const { config } = require('../Config');
const logger = require('../utilities/logger');
const constants = require('../../constants');
const bucketclient = require('bucketclient');
const { instrumentMetadata } = require('../instrumentation/simple');

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

const metadata = new MetadataWrapper(config.backends.metadata, params,
    bucketclient, logger);

// Apply comprehensive metadata instrumentation for all backends
const instrumentedMetadata = instrumentMetadata(metadata);

const originalSetup = instrumentedMetadata.setup;
if (originalSetup) {
    instrumentedMetadata.setup = function (callback) {
        return originalSetup.call(this, (err, result) => callback(err, result));
    };
}

// MongoDB-specific legacy logging removed - now handled by simple instrumentation

module.exports = instrumentedMetadata;
