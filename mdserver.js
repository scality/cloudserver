'use strict'; // eslint-disable-line strict
require('babel-core/register');

const config = require('./lib/Config.js').default;
const MetadataFileServer =
          require('arsenal').storage.metadata.MetadataFileServer;

if (config.backends.metadata === 'file') {
    const mdServer = new MetadataFileServer(
        { bindAddress: config.metadataDaemon.bindAddress,
          port: config.metadataDaemon.port,
          path: config.metadataDaemon.metadataPath,
          log: config.log,
          versioning: { replicationGroupId: config.replicationGroupId } });
    mdServer.startServer();
}

