const parseXML = require('../../../utilities/parseXML');
const ReplicationConfiguration = require('./models/ReplicationConfiguration');

// Handle the steps for returning a valid replication configuration object.
function getReplicationConfiguration(xml, log, cb) {
    return parseXML(xml, log, (err, result) => {
        if (err) {
            return cb(err);
        }
        const validator = new ReplicationConfiguration(result, log);
        const configErr = validator.parseConfiguration();
        return cb(configErr || null, validator.getReplicationConfiguration());
    });
}

module.exports = getReplicationConfiguration;
