const config = require('../../../Config').config;
const parseXML = require('../../../utilities/parseXML');
const ReplicationConfiguration =
  require('arsenal').models.ReplicationConfiguration;

// Handle the steps for returning a valid replication configuration object.
function getReplicationConfiguration(xml, log, cb) {
    return parseXML(xml, log, (err, result) => {
        if (err) {
            return cb(err);
        }
        const validator = new ReplicationConfiguration(result, log, config);
        const configErr = validator.parseConfiguration();
        return cb(configErr || null, validator.getReplicationConfiguration());
    });
}

// Get the XML representation of the bucket replication configuration.
function getReplicationConfigurationXML(config) {
    return ReplicationConfiguration.getConfigXML(config);
}

module.exports = {
    getReplicationConfiguration,
    getReplicationConfigurationXML,
};
