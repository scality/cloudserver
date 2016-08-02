import assert from 'assert';
import BucketInfo from '../../../lib/metadata/BucketInfo';
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
            });

        describe('serialize/deSerialize on BucketInfo class', () => {
            let serialized;
            it('should serialize', done => {
                serialized = dummyBucket.serialize();
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
                };
                assert.strictEqual(serialized, JSON.stringify(bucketInfos));
                done();
            });

            it('should deSerialize into an  instance of BucketInfo', done => {
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
                       dummyBucket.locationConstraint, newLocation);
               });
        });
    })
);
