const assert = require('assert');

const { DummyRequestLogger } = require('../helpers');
const { getReplicationConfiguration } =
    require('../../../lib/api/apiUtils/bucket/getReplicationConfiguration');
const validateReplicationConfig =
    require('../../../lib/api/apiUtils/bucket/validateReplicationConfig');
const replicationUtils =
    require('../../functional/aws-node-sdk/lib/utility/replication');
const log = new DummyRequestLogger();

// Check for the expected error response code and status code.
function checkError(xml, expectedErr, cb) {
    getReplicationConfiguration(xml, log, err => {
        if (expectedErr === null) {
            expect(err).toBe(null);
        } else {
            expect(err[expectedErr]).toBeTruthy();
        }
        return cb();
    });
}

// Check that the ID has been created properly.
function checkGeneratedID(xml, cb) {
    getReplicationConfiguration(xml, log, (err, res) => {
        if (err) {
            return cb(err);
        }
        const id = res.rules[0].id;
        expect(typeof id).toBe('string');
        expect(id.length).toBe(48);
        return cb();
    });
}

// Create replication configuration XML with an tag optionally omitted.
function createReplicationXML(missingTag, tagValue) {
    let Role = missingTag === 'Role' ? '' :
        '<Role>' +
            'arn:aws:iam::account-id:role/src-resource,' +
            'arn:aws:iam::account-id:role/dest-resource' +
        '</Role>';
    Role = tagValue && tagValue.Role ? `<Role>${tagValue.Role}</Role>` : Role;
    let ID = missingTag === 'ID' ? '' : '<ID>foo</ID>';
    ID = tagValue && tagValue.ID === '' ? '<ID/>' : ID;
    const Prefix = missingTag === 'Prefix' ? '' : '<Prefix>foo</Prefix>';
    const Status = missingTag === 'Status' ? '' : '<Status>Enabled</Status>';
    const Bucket = missingTag === 'Bucket' ? '' :
        '<Bucket>arn:aws:s3:::destination-bucket</Bucket>';
    let StorageClass = missingTag === 'StorageClass' ? '' :
        '<StorageClass>STANDARD</StorageClass>';
    StorageClass = tagValue && tagValue.StorageClass ?
        `<StorageClass>${tagValue.StorageClass}</StorageClass>` : StorageClass;
    const Destination = missingTag === 'Destination' ? '' :
        `<Destination>${Bucket + StorageClass}</Destination>`;
    const Rule = missingTag === 'Rule' ? '' :
        `<Rule>${ID + Prefix + Status + Destination}</Rule>`;
    const content = missingTag === null ? '' : `${Role}${Rule}`;
    return '<ReplicationConfiguration ' +
            `xmlns="http://s3.amazonaws.com/doc/2006-03-01/">${content}` +
        '</ReplicationConfiguration>';
}

describe('\'getReplicationConfiguration\' function', () => {
    test('should not return error when putting valid XML', done =>
        checkError(createReplicationXML(), null, done));

    test('should not accept empty replication configuration', done =>
        checkError(createReplicationXML(null), 'MalformedXML', done));

    replicationUtils.requiredConfigProperties.forEach(prop => {
        // Note that the XML uses 'Rule' while the config object uses 'Rules'.
        const xmlTag = prop === 'Rules' ? 'Rule' : prop;
        const xml = createReplicationXML(xmlTag);

        test(
            `should not accept replication configuration without '${prop}'`,
            done => checkError(xml, 'MalformedXML', done)
        );
    });

    replicationUtils.optionalConfigProperties.forEach(prop => {
        test(
            `should accept replication configuration without '${prop}'`,
            done => checkError(createReplicationXML(prop), null, done)
        );
    });

    test(`should accept replication configuration without 'Bucket' when there
    is no Scality destination in the Storage Class`, done => {
        const xml = createReplicationXML('Bucket', {
            StorageClass: 'us-east-2',
            Role: 'arn:aws:iam::account-id:role/src-resource',
        });
        checkError(xml, null, done);
    });

    test("should create a rule 'ID' if omitted from the replication " +
    'configuration', done => {
        const xml = createReplicationXML('ID');
        return checkGeneratedID(xml, done);
    });

    test('should create an \'ID\' if rule ID is \'\'', done => {
        const xml = createReplicationXML(undefined, { ID: '' });
        return checkGeneratedID(xml, done);
    });
});

describe('\'validateReplicationConfig\' function', () => {
    const nonTransientBucket = {
        getLocationConstraint: () => 'us-east-1',
    };
    const transientBucket = {
        getLocationConstraint: () => 'transientfile',
    };

    test('should validate configuration when bucket location is ' +
    'not transient and preferred read location is not specified', () => {
        const withoutPreferredRead = {
            role: 'arn:aws:iam::account-id:role/src-resource,' +
                'arn:aws:iam::account-id:role/dest-resource',
            destination: 'arn:aws:s3:::destination-bucket',
            rules: [{
                prefix: 'test-prefix',
                enabled: true,
                id: 'test-id',
                storageClass: 'STANDARD,us-east-2',
            }],
        };
        const result = validateReplicationConfig(withoutPreferredRead,
                                                 nonTransientBucket);
        expect(result).toBe(true);
    });

    test('should validate configuration when bucket location is transient ' +
    'and preferred read location is specified', () => {
        const withPreferredRead = {
            role: 'arn:aws:iam::account-id:role/src-resource,' +
                'arn:aws:iam::account-id:role/dest-resource',
            destination: 'arn:aws:s3:::destination-bucket',
            rules: [{
                prefix: 'test-prefix',
                enabled: true,
                id: 'test-id',
                storageClass: 'STANDARD,us-east-2:preferred_read',
            }],
        };
        const result = validateReplicationConfig(withPreferredRead,
                                                 transientBucket);
        expect(result).toBe(true);
    });

    test('should not validate configuration when bucket location is ' +
    'transient and preferred read location is not specified', () => {
        const withoutPreferredRead = {
            role: 'arn:aws:iam::account-id:role/src-resource,' +
                'arn:aws:iam::account-id:role/dest-resource',
            destination: 'arn:aws:s3:::destination-bucket',
            rules: [{
                prefix: 'test-prefix',
                enabled: true,
                id: 'test-id',
                storageClass: 'STANDARD,us-east-2',
            }],
        };
        const result = validateReplicationConfig(withoutPreferredRead,
                                                 transientBucket);
        expect(result).toBe(false);
    });
});
