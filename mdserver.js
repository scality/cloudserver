'use strict'; // eslint-disable-line strict

const { config } = require('./lib/Config.js');
const MetadataFileServer =
          require('arsenal').storage.metadata.MetadataFileServer;

if (config.backends.metadata === 'file') {
    const mdServer = new MetadataFileServer(
        { bindAddress: config.metadataDaemon.bindAddress,
          port: config.metadataDaemon.port,
          path: config.metadataDaemon.metadataPath,
          versioning: { replicationGroupId: config.replicationGroupId },
          recordLog: config.recordLog,
          log: config.log });
    mdServer.startServer();
}
