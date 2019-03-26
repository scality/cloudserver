const assert = require('assert');
const { parseString } = require('xml2js');

const { DummyRequestLogger } = require('../helpers');
const { getReplicationConfigurationXML } =
    require('../../../lib/api/apiUtils/bucket/getReplicationConfiguration');

// Compare the values from the parsedXML with the original configuration values.
function checkXML(parsedXML, config) {
    const { Rule, Role } = parsedXML.ReplicationConfiguration;
    const { role, destination } = config;
    expect(Role[0]).toBe(role);
    expect(Array.isArray(Rule)).toBe(true);
    for (let i = 0; i < Rule.length; i++) {
        const { ID, Prefix, Status, Destination } = Rule[i];
        const { Bucket, StorageClass } = Destination[0];
        const { id, prefix, enabled, storageClass } = config.rules[i];
        expect(ID[0]).toBe(id);
        expect(Prefix[0]).toBe(prefix);
        expect(Status[0]).toBe(enabled ? 'Enabled' : 'Disabled');
        if (storageClass !== undefined) {
            expect(StorageClass[0]).toBe(storageClass);
        }
        expect(Bucket[0]).toBe(destination);
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
        role: 'arn:aws:iam::account-id:role/resource',
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
    test('should return XML from the bucket replication configuration', done =>
        getAndCheckXML(getReplicationConfig(), done));

    test('should not return XML with StorageClass tag if `storageClass` ' +
    'property is omitted', done => {
        const config = getReplicationConfig();
        delete config.rules[0].storageClass;
        return getAndCheckXML(config, done);
    });

    test("should return XML with StorageClass tag set to 'Disabled' if " +
        '`enabled` property is false', done => {
        const config = getReplicationConfig();
        config.rules[0].enabled = false;
        return getAndCheckXML(config, done);
    });

    test('should return XML with a self-closing Prefix tag if `prefix` ' +
    "property is ''", done => {
        const config = getReplicationConfig();
        config.rules[0].prefix = '';
        return getAndCheckXML(config, done);
    });

    test('should return XML from the bucket replication configuration with ' +
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
