const MetadataWrapper = require('arsenal').storage.metadata.MetadataWrapper;
const { config } = require('../Config');
const logger = require('../utilities/logger');
const constants = require('../../constants');
const bucketclient = require('bucketclient');

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
// call setup
metadata.setup(() => {});

module.exports = metadata;
