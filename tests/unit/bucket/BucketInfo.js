const assert = require('assert');
const BucketInfo = require('../../../lib/metadata/BucketInfo');
const { WebsiteConfiguration }
    = require('../../../lib/metadata/WebsiteConfiguration');
// create variables to populate dummyBucket
const bucketName = 'nameOfBucket';
const owner = 'canonicalID';
const ownerDisplayName = 'bucketOwner';
const emptyAcl = {
    Canned: 'private',
    FULL_CONTROL: [],
    WRITE: [],
    WRITE_ACP: [],
    READ: [],
    READ_ACP: [],
};

const filledAcl = {
    Canned: '',
    FULL_CONTROL: ['someOtherAccount'],
    WRITE: [],
    WRITE_ACP: ['yetAnotherAccount'],
    READ: [],
    READ_ACP: ['thisaccount'],
};

const acl = { undefined, emptyAcl, filledAcl };

const testDate = new Date().toJSON();

const testVersioningConfiguration = { Status: 'Enabled' };

const testWebsiteConfiguration = new WebsiteConfiguration({
    indexDocument: 'index.html',
    errorDocument: 'error.html',
    routingRules: [
        {
            redirect: {
                httpRedirectCode: '301',
                hostName: 'www.example.com',
                replaceKeyPrefixWith: '/documents',
            },
            condition: {
                httpErrorCodeReturnedEquals: 400,
                keyPrefixEquals: '/docs',
            },
        },
        {
            redirect: {
                protocol: 'http',
                replaceKeyWith: 'error.html',
            },
            condition: {
                keyPrefixEquals: 'ExamplePage.html',
            },
        },
    ],
});

const testLocationConstraint = 'us-west-1';

const testCorsConfiguration = [
    { id: 'test',
        allowedMethods: ['PUT', 'POST', 'DELETE'],
        allowedOrigins: ['http://www.example.com'],
        allowedHeaders: ['*'],
        maxAgeSeconds: 3000,
        exposeHeaders: ['x-amz-server-side-encryption'] },
    { allowedMethods: ['GET'],
        allowedOrigins: ['*'],
        allowedHeaders: ['*'],
        maxAgeSeconds: 3000 },
];

const testReplicationConfiguration = {
    role: 'STRING_VALUE',
    destination: 'STRING_VALUE',
    rules: [
        {
            storageClass: 'STANDARD',
            prefix: 'STRING_VALUE',
            enabled: true,
            id: 'STRING_VALUE',
        },
    ],
};
// create a dummy bucket to test getters and setters

Object.keys(acl).forEach(
    aclObj => describe(`different acl configurations : ${aclObj}`, () => {
        const dummyBucket = new BucketInfo(
            bucketName, owner, ownerDisplayName, testDate,
            BucketInfo.currentModelVersion(), acl[aclObj],
            false, false, {
                cryptoScheme: 1,
                algorithm: 'sha1',
                masterKeyId: 'somekey',
                mandatory: true,
            }, testVersioningConfiguration,
            testLocationConstraint,
            testWebsiteConfiguration,
            testCorsConfiguration,
            testReplicationConfiguration);

        describe('serialize/deSerialize on BucketInfo class', () => {
            const serialized = dummyBucket.serialize();
            it('should serialize', done => {
                assert.strictEqual(typeof serialized, 'string');
                const bucketInfos = {
                    acl: dummyBucket._acl,
                    name: dummyBucket._name,
                    owner: dummyBucket._owner,
                    ownerDisplayName: dummyBucket._ownerDisplayName,
                    creationDate: dummyBucket._creationDate,
                    mdBucketModelVersion: dummyBucket._mdBucketModelVersion,
                    transient: dummyBucket._transient,
                    deleted: dummyBucket._deleted,
                    serverSideEncryption: dummyBucket._serverSideEncryption,
                    versioningConfiguration:
                        dummyBucket._versioningConfiguration,
                    locationConstraint: dummyBucket._locationConstraint,
                    websiteConfiguration: dummyBucket._websiteConfiguration
                        .getConfig(),
                    cors: dummyBucket._cors,
                    replicationConfiguration:
                        dummyBucket._replicationConfiguration,
                };
                assert.strictEqual(serialized, JSON.stringify(bucketInfos));
                done();
            });

            it('should deSerialize into an instance of BucketInfo', done => {
                const serialized = dummyBucket.serialize();
                const deSerialized = BucketInfo.deSerialize(serialized);
                assert.strictEqual(typeof deSerialized, 'object');
                assert(deSerialized instanceof BucketInfo);
                assert.deepStrictEqual(deSerialized, dummyBucket);
                done();
            });
        });

        describe('constructor', () => {
            it('this should have the right BucketInfo types',
               () => {
                   assert.strictEqual(typeof dummyBucket.getName(), 'string');
                   assert.strictEqual(typeof dummyBucket.getOwner(), 'string');
                   assert.strictEqual(typeof dummyBucket.getOwnerDisplayName(),
                                      'string');
                   assert.strictEqual(typeof dummyBucket.getCreationDate(),
                                      'string');
               });
            it('this should have the right acl\'s types', () => {
                assert.strictEqual(typeof dummyBucket.getAcl(), 'object');
                assert.strictEqual(
                    typeof dummyBucket.getAcl().Canned, 'string');
                assert(Array.isArray(dummyBucket.getAcl().FULL_CONTROL));
                assert(Array.isArray(dummyBucket.getAcl().WRITE));
                assert(Array.isArray(dummyBucket.getAcl().WRITE_ACP));
                assert(Array.isArray(dummyBucket.getAcl().READ));
                assert(Array.isArray(dummyBucket.getAcl().READ_ACP));
            });
            it('this should have the right acls', () => {
                assert.deepStrictEqual(dummyBucket.getAcl(),
                                       acl[aclObj] || emptyAcl);
            });
            it('this should have the right website config types', () => {
                const websiteConfig = dummyBucket.getWebsiteConfiguration();
                assert.strictEqual(typeof websiteConfig, 'object');
                assert.strictEqual(typeof websiteConfig._indexDocument,
                    'string');
                assert.strictEqual(typeof websiteConfig._errorDocument,
                    'string');
                assert(Array.isArray(websiteConfig._routingRules));
            });
            it('this should have the right cors config types', () => {
                const cors = dummyBucket.getCors();
                assert(Array.isArray(cors));
                assert(Array.isArray(cors[0].allowedMethods));
                assert(Array.isArray(cors[0].allowedOrigins));
                assert(Array.isArray(cors[0].allowedHeaders));
                assert(Array.isArray(cors[0].allowedMethods));
                assert(Array.isArray(cors[0].exposeHeaders));
                assert.strictEqual(typeof cors[0].maxAgeSeconds, 'number');
                assert.strictEqual(typeof cors[0].id, 'string');
            });
        });

        describe('getters on BucketInfo class', () => {
            it('getACl should return the acl', () => {
                assert.deepStrictEqual(dummyBucket.getAcl(),
                                       acl[aclObj] || emptyAcl);
            });
            it('getName should return name', () => {
                assert.deepStrictEqual(dummyBucket.getName(), bucketName);
            });
            it('getOwner should return owner', () => {
                assert.deepStrictEqual(dummyBucket.getOwner(), owner);
            });
            it('getOwnerDisplayName should return ownerDisplayName', () => {
                assert.deepStrictEqual(dummyBucket.getOwnerDisplayName(),
                                       ownerDisplayName);
            });
            it('getCreationDate should return creationDate', () => {
                assert.deepStrictEqual(dummyBucket.getCreationDate(), testDate);
            });
            it('getVersioningConfiguration should return configuration', () => {
                assert.deepStrictEqual(dummyBucket.getVersioningConfiguration(),
                        testVersioningConfiguration);
            });
            it('getWebsiteConfiguration should return configuration', () => {
                assert.deepStrictEqual(dummyBucket.getWebsiteConfiguration(),
                        testWebsiteConfiguration);
            });
            it('getLocationConstraint should return locationConstraint', () => {
                assert.deepStrictEqual(dummyBucket.getLocationConstraint(),
                testLocationConstraint);
            });
            it('getCors should return CORS configuration', () => {
                assert.deepStrictEqual(dummyBucket.getCors(),
                        testCorsConfiguration);
            });
        });

        describe('setters on BucketInfo class', () => {
            it('setCannedAcl should set acl.Canned', () => {
                const testAclCanned = 'public-read';
                dummyBucket.setCannedAcl(testAclCanned);
                assert.deepStrictEqual(
                    dummyBucket.getAcl().Canned, testAclCanned);
            });
            it('setSpecificAcl should set the acl of a specified bucket',
               () => {
                   const typeOfGrant = 'WRITE';
                   dummyBucket.setSpecificAcl(owner, typeOfGrant);
                   const lastIndex =
                             dummyBucket.getAcl()[typeOfGrant].length - 1;
                   assert.deepStrictEqual(
                       dummyBucket.getAcl()[typeOfGrant][lastIndex], owner);
               });
            it('setFullAcl should set full set of ACLs', () => {
                const newACLs = {
                    Canned: '',
                    FULL_CONTROL: ['someOtherAccount'],
                    WRITE: [],
                    WRITE_ACP: ['yetAnotherAccount'],
                    READ: [],
                    READ_ACP: [],
                };
                dummyBucket.setFullAcl(newACLs);
                assert.deepStrictEqual(dummyBucket.getAcl().FULL_CONTROL,
                                       ['someOtherAccount']);
                assert.deepStrictEqual(dummyBucket.getAcl().WRITE_ACP,
                                       ['yetAnotherAccount']);
            });
            it('setName should set the bucket name', () => {
                const newName = 'newName';
                dummyBucket.setName(newName);
                assert.deepStrictEqual(dummyBucket.getName(), newName);
            });
            it('setOwner should set the owner', () => {
                const newOwner = 'newOwner';
                dummyBucket.setOwner(newOwner);
                assert.deepStrictEqual(dummyBucket.getOwner(), newOwner);
            });
            it('getOwnerDisplayName should return ownerDisplayName', () => {
                const newOwnerDisplayName = 'newOwnerDisplayName';
                dummyBucket.setOwnerDisplayName(newOwnerDisplayName);
                assert.deepStrictEqual(dummyBucket.getOwnerDisplayName(),
                                       newOwnerDisplayName);
            });
            it('setLocationConstraint should set the locationConstraint',
               () => {
                   const newLocation = 'newLocation';
                   dummyBucket.setLocationConstraint(newLocation);
                   assert.deepStrictEqual(
                       dummyBucket.getLocationConstraint(), newLocation);
               });
            it('setVersioningConfiguration should set configuration', () => {
                const newVersioningConfiguration =
                    { Status: 'Enabled', MfaDelete: 'Enabled' };
                dummyBucket
                    .setVersioningConfiguration(newVersioningConfiguration);
                assert.deepStrictEqual(dummyBucket.getVersioningConfiguration(),
                    newVersioningConfiguration);
            });
            it('setWebsiteConfiguration should set configuration', () => {
                const newWebsiteConfiguration = {
                    redirectAllRequestsTo: {
                        hostName: 'www.example.com',
                        protocol: 'https',
                    },
                };
                dummyBucket
                    .setWebsiteConfiguration(newWebsiteConfiguration);
                assert.deepStrictEqual(dummyBucket.getWebsiteConfiguration(),
                    newWebsiteConfiguration);
            });
            it('setCors should set CORS configuration', () => {
                const newCorsConfiguration =
                    [{ allowedMethods: ['PUT'], allowedOrigins: ['*'] }];
                dummyBucket.setCors(newCorsConfiguration);
                assert.deepStrictEqual(dummyBucket.getCors(),
                    newCorsConfiguration);
            });
            it('setReplicationConfiguration should set replication ' +
                'configuration', () => {
                const newReplicationConfig = {
                    Role: 'arn:aws:iam::123456789012:role/src-resource,' +
                        'arn:aws:iam::123456789012:role/dest-resource',
                    Rules: [
                        {
                            Destination: {
                                Bucket: 'arn:aws:s3:::destination-bucket',
                            },
                            Prefix: 'test-prefix',
                            Status: 'Enabled',
                        },
                    ],
                };
                dummyBucket.setReplicationConfiguration(newReplicationConfig);
            });
        });
    })
);
