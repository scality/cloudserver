const assert = require('assert');
const { errors } = require('arsenal');

const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketPutReplication = require('../../../lib/api/bucketPutReplication');
const bucketPutVersioning = require('../../../lib/api/bucketPutVersioning');
const { cleanup, DummyRequestLogger, makeAuthInfo } = require('../helpers');
const { getReplicationConfiguration } =
    require('../../../lib/api/apiUtils/bucket/getReplicationConfiguration');
const replicationUtils =
    require('../../functional/aws-node-sdk/lib/utility/replication');
const log = new DummyRequestLogger();

const authInfo = makeAuthInfo('accessKey1');
const bucketName = 'bucketname';
const testBucketPutRequest = {
    bucketName,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
    actionImplicitDenies: false,
};

// Check for the expected error response code and status code.
function checkError(xml, expectedErr, cb) {
    getReplicationConfiguration(xml, log, err => {
        if (expectedErr === null) {
            assert.strictEqual(err, null, `expected no error but got '${err}'`);
        } else {
            assert(err.is[expectedErr], 'incorrect error response: should be ' +
                `'Error: ${expectedErr}' but got '${err}'`);
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
        assert.strictEqual(typeof id, 'string', 'expected rule ID to be ' +
            `string but got ${typeof id}`);
        assert.strictEqual(id.length, 48, 'expected rule ID to be a length ' +
            `of 48 but got ${id.length}`);
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
    it('should not return error when putting valid XML', done =>
        checkError(createReplicationXML(), null, done));

    it('should not accept empty replication configuration', done =>
        checkError(createReplicationXML(null), 'MalformedXML', done));

    replicationUtils.requiredConfigProperties.forEach(prop => {
        // Note that the XML uses 'Rule' while the config object uses 'Rules'.
        const xmlTag = prop === 'Rules' ? 'Rule' : prop;
        const xml = createReplicationXML(xmlTag);

        it(`should not accept replication configuration without '${prop}'`,
            done => checkError(xml, 'MalformedXML', done));
    });

    replicationUtils.optionalConfigProperties.forEach(prop => {
        it(`should accept replication configuration without '${prop}'`,
            done => checkError(createReplicationXML(prop), null, done));
    });

    it(`should accept replication configuration without 'Bucket' when there
    is no Scality destination in the Storage Class`, done => {
        const xml = createReplicationXML('Bucket', {
            StorageClass: 'us-east-2',
            Role: 'arn:aws:iam::account-id:role/src-resource',
        });
        checkError(xml, null, done);
    });

    it("should create a rule 'ID' if omitted from the replication " +
    'configuration', done => {
        const xml = createReplicationXML('ID');
        return checkGeneratedID(xml, done);
    });

    it('should create an \'ID\' if rule ID is \'\'', done => {
        const xml = createReplicationXML(undefined, { ID: '' });
        return checkGeneratedID(xml, done);
    });
});

describe('bucketPutReplication API - Content-MD5 validation', () => {
    const replicationXML = createReplicationXML();

    before(() => cleanup());
    beforeEach(done => {
        // Create bucket first
        bucketPut(authInfo, testBucketPutRequest, log, err => {
            if (err) {
                return done(err);
            }
            // Enable versioning (required for replication)
            const versioningRequest = {
                bucketName,
                headers: { host: `${bucketName}.s3.amazonaws.com` },
                post: '<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>',
                url: '/?versioning',
                query: { versioning: '' },
                actionImplicitDenies: false,
            };
            return bucketPutVersioning(authInfo, versioningRequest, log, done);
        });
    });
    afterEach(() => cleanup());

    it('should not return an error when Content-MD5 header is missing', done => {
        const testReplicationRequest = {
            bucketName,
            headers: { host: `${bucketName}.s3.amazonaws.com` },
            post: replicationXML,
            url: '/?replication',
            query: { replication: '' },
            actionImplicitDenies: false,
        };

        bucketPutReplication(authInfo, testReplicationRequest, log, err => {
            assert.ifError(err);
            done();
        });
    });

    it('should return BadDigest error when Content-MD5 header mismatches', done => {
        const testReplicationRequest = {
            bucketName,
            headers: {
                'host': `${bucketName}.s3.amazonaws.com`,
                'content-md5': '+5yj3kZsXledyKr18eaUDg==', // incorrect MD5
            },
            post: replicationXML,
            url: '/?replication',
            query: { replication: '' },
            actionImplicitDenies: false,
        };

        bucketPutReplication(authInfo, testReplicationRequest, log, err => {
            assert.deepStrictEqual(err, errors.BadDigest);
            done();
        });
    });

    it('should not return an error when Content-MD5 header matches', done => {
        const testReplicationRequest = {
            bucketName,
            headers: {
                'host': `${bucketName}.s3.amazonaws.com`,
                'content-md5': 'IKwQ83x91j3jaIvsiKstUQ==', // correct MD5
            },
            post: replicationXML,
            url: '/?replication',
            query: { replication: '' },
            actionImplicitDenies: false,
        };

        bucketPutReplication(authInfo, testReplicationRequest, log, err => {
            assert.ifError(err);
            done();
        });
    });
});
