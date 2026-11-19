const config = require('../../../Config').config;
const parseXML = require('../../../utilities/parseXML');
const ReplicationConfiguration = require('arsenal').models.ReplicationConfiguration;

/**
 * We tell if a rule replicates to a CRR site: a replication endpoint with no
 * type. A storageClass that is not an endpoint (none, or STANDARD) means the
 * default endpoint.
 * @param {object[]} rules - The rules of the replication configuration
 * @return {boolean} true when at least one rule replicates to a CRR site
 */
function _hasCrrSite(rules) {
    const { replicationEndpoints } = config;
    const defaultEndpoint = replicationEndpoints.find(endpoint => endpoint.default) ?? replicationEndpoints[0];
    const storageClasses = rules.flatMap(rule => (rule.storageClass ?? '').split(','));
    return storageClasses.some(storageClass => {
        const site = storageClass.split(':')[0];
        const endpoint = replicationEndpoints.find(endpoint => endpoint.site === site) ?? defaultEndpoint;
        return endpoint !== undefined && endpoint.type === undefined;
    });
}

/**
 * We rewrite the replication configuration like a cloudserver older than
 * 9.4 stores it: one destination bucket for all the rules. An old
 * cloudserver crashes on the new shape.
 * We refuse what it refuses: a single role for a CRR site, rules to
 * different buckets, overlapping prefixes. Like it, we ignore the account
 * of a rule.
 * @param {object} replicationConfig - The configuration in the current shape
 * @return {object} { refused: { code, description } }, or { replicationConfig } in the old shape
 */
function toLegacyConfiguration(replicationConfig) {
    const { role, rules, preferredReadLocation } = replicationConfig;
    const refuse = (code, description) => ({ refused: { code, description } });
    if (role.split(',').length !== 2 && _hasCrrSite(rules)) {
        return refuse(
            'InvalidArgument',
            'Invalid Role specified in replication configuration: ' +
                'Role must be a comma-separated list of two IAM roles',
        );
    }
    if (rules.some(rule => rule.destination !== rules[0].destination)) {
        return refuse('InvalidRequest', 'The destination bucket must be the same for all rules');
    }
    const prefixes = rules.map(rule => rule.prefix ?? '');
    for (let i = 0; i < prefixes.length; i++) {
        const overlapping = prefixes.slice(0, i).find(p => p.startsWith(prefixes[i]) || prefixes[i].startsWith(p));
        if (overlapping !== undefined) {
            return refuse('InvalidRequest', `Found overlapping prefixes '${overlapping}' and '${prefixes[i]}'`);
        }
    }
    return {
        replicationConfig: {
            role,
            destination: rules[0].destination,
            rules: rules.map(({ prefix, enabled, id, storageClass }) => ({
                prefix: prefix ?? '',
                enabled,
                id,
                storageClass,
            })),
            preferredReadLocation,
        },
    };
}

// Handle the steps for returning a valid replication configuration object.
function getReplicationConfiguration(xml, log, cb) {
    return parseXML(xml, log, (err, result) => {
        if (err) {
            return cb(err);
        }

        const validator = new ReplicationConfiguration(result, log, config);
        const configErr = validator.parseConfiguration();

        if (configErr) {
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
    toLegacyConfiguration,
    getReplicationConfigurationXML,
};
