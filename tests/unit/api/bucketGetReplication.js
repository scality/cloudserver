const assert = require('assert');
const { parseString } = require('xml2js');

const { DummyRequestLogger } = require('../helpers');
const { getReplicationConfigurationXML } =
    require('../../../lib/api/apiUtils/bucket/getReplicationConfiguration');

// Compare the values from the parsedXML with the original configuration values.
function checkXML(parsedXML, config) {
    const { Rule, Role } = parsedXML.ReplicationConfiguration;
    const { role, destination } = config;
    assert.strictEqual(Role[0], role);
    assert.strictEqual(Array.isArray(Rule), true);
    for (let i = 0; i < Rule.length; i++) {
        const { ID, Prefix, Status, Destination } = Rule[i];
        const { Bucket, StorageClass } = Destination[0];
        const { id, prefix, enabled, storageClass } = config.rules[i];
        assert.strictEqual(ID[0], id);
        assert.strictEqual(Prefix[0], prefix);
        assert.strictEqual(Status[0], enabled ? 'Enabled' : 'Disabled');
        if (storageClass !== undefined) {
            assert.strictEqual(StorageClass[0], storageClass);
        }
        assert.strictEqual(Bucket[0], destination);
    }
}

// Get the replication XML, parse it, and check that values are as expected.
function getAndCheckXML(bucketReplicationConfig, cb) {
    const log = new DummyRequestLogger();
    const xml = getReplicationConfigurationXML(bucketReplicationConfig, log);
    return parseString(xml, (err, res) => {
        if (err) {
            return cb(err);
        }
        checkXML(res, bucketReplicationConfig);
        return cb(null, res);
    });
}

// Get an example bucket replication configuration.
function getReplicationConfig() {
    return {
        role: 'arn:partition:service::account-id:resourcetype/resource',
        destination: 'destination-bucket',
        rules: [
            {
                id: 'test-id-1',
                prefix: 'test-prefix-1',
                enabled: true,
                storageClass: 'STANDARD',
            },
        ],
    };
}

describe("'getReplicationConfigurationXML' function", () => {
    it('should return XML from the bucket replication configuration', done =>
        getAndCheckXML(getReplicationConfig(), done));

    it('should not return XML with StorageClass tag if `storageClass` ' +
    'property is omitted', done => {
        const config = getReplicationConfig();
        delete config.rules[0].storageClass;
        return getAndCheckXML(config, done);
    });

    it("should return XML with StorageClass tag set to 'Disabled' if " +
        '`enabled` property is false', done => {
        const config = getReplicationConfig();
        config.rules[0].enabled = false;
        return getAndCheckXML(config, done);
    });

    it('should return XML with a self-closing Prefix tag if `prefix` ' +
    "property is ''", done => {
        const config = getReplicationConfig();
        config.rules[0].prefix = '';
        return getAndCheckXML(config, done);
    });

    it('should return XML from the bucket replication configuration with ' +
    'multiple rules', done => {
        const config = getReplicationConfig();
        config.rules.push({
            id: 'test-id-2',
            prefix: 'test-prefix-2',
            enabled: true,
        });
        return getAndCheckXML(config, done);
    });
});
