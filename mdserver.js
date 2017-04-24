'use strict'; // eslint-disable-line strict

const { config } = require('./lib/Config.js');
const MetadataFileServer =
          require('arsenal').storage.metadata.MetadataFileServer;

if (config.backends.metadata === 'file') {
    const mdServer = new MetadataFileServer(
        { bindAddress: config.metadataDaemon.bindAddress,
          port: config.metadataDaemon.port,
          path: config.metadataDaemon.metadataPath,
          restEnabled: config.metadataDaemon.restEnabled,
          restPort: config.metadataDaemon.restPort,
          log: config.log,
          versioning: { replicationGroupId: config.replicationGroupId } });
    mdServer.startServer();
}
