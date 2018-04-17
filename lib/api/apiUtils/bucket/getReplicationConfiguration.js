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
        if (configErr) {
            log.trace('replication configuration failed validation', {
                xml, method: 'getReplicationConfiguration',
            });
            return cb(configErr);
        }
        return cb(null, validator.getReplicationConfiguration());
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
