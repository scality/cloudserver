const assert = require('assert');
const Promise = require('bluebird');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const constants = require('../../../../../constants');

const notOwnerCanonicalID = '79a59df900b949e55d96a1e698fba' +
    'cedfd6e09d98eacf8f8d5218e7cd47ef2bf';
const itSkipIfAWS = process.env.AWS_ON_AIR ? it.skip : it;

class _AccessControlPolicy {
    constructor(params) {
        this.Owner = {};
        this.Owner.ID = params.ownerID;
        if (params.ownerDisplayName) {
            this.Owner.DisplayName = params.ownerDisplayName;
        }
        this.Grants = [];
    }
    addGrantee(type, value, permission, displayName) {
        const grant = {
            Grantee: {
                Type: type,
            },
            Permission: permission,
        };
        if (displayName) {
            grant.Grantee.DisplayName = displayName;
        }
        if (type === 'AmazonCustomerByEmail') {
            grant.Grantee.EmailAddress = value;
        } else if (type === 'CanonicalUser') {
            grant.Grantee.ID = value;
        } else if (type === 'Group') {
            grant.Grantee.URI = value;
        }
        this.Grants.push(grant);
    }
}

describe('PUT Object ACL', () => {
    withV4(sigCfg => {
        let bucketName;
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        const Key = 'aclTest';

        beforeAll(done => {
            bucketUtil.createRandom(1)
                      .then(created => {
                          bucketName = created;
                          done();
                      })
                      .catch(done);
        });

        afterEach(() => {
            process.stdout.write('emptying bucket');
            return bucketUtil.empty(bucketName);
        });

        afterAll(() => {
            process.stdout.write('deleting bucket');
            return bucketUtil.deleteOne(bucketName);
        });

        test('should put object ACLs', done => {
            const s3 = bucketUtil.s3;
            const Bucket = bucketName;
            const objects = [
                { Bucket, Key },
            ];

            Promise
                .mapSeries(objects, param => s3.putObjectAsync(param))
                .then(() => s3.putObjectAclAsync({ Bucket, Key,
                    ACL: 'public-read' }))
                .then(data => {
                    expect(data).toBeTruthy();
                    done();
                })
                .catch(done);
        });

        test('should return NoSuchKey if try to put object ACLs ' +
            'for nonexistent object', done => {
            const s3 = bucketUtil.s3;
            const Bucket = bucketName;

            s3.putObjectAcl({
                Bucket,
                Key,
                ACL: 'public-read' }, err => {
                expect(err).toBeTruthy();
                expect(err.statusCode).toBe(404);
                expect(err.code).toBe('NoSuchKey');
                done();
            });
        });

        describe('on an object', () => {
            beforeAll(done => s3.putObject({ Bucket: bucketName, Key }, done));
            afterAll(() => {
                process.stdout.write('deleting bucket');
                return bucketUtil.empty(bucketName);
            });
            // The supplied canonical ID is not associated with a real AWS
            // account, so AWS_ON_AIR will raise a 400 InvalidArgument
            itSkipIfAWS('should return AccessDenied if try to change owner ' +
                'ID in ACL request body', done => {
                const acp = new _AccessControlPolicy(
                    { ownerID: notOwnerCanonicalID });
                acp.addGrantee('Group', constants.publicId, 'READ');
                const putAclParams = {
                    Bucket: bucketName,
                    Key,
                    AccessControlPolicy: acp,
                };
                s3.putObjectAcl(putAclParams, err => {
                    expect(err).toBeTruthy();
                    expect(err.statusCode).toBe(403);
                    expect(err.code).toBe('AccessDenied');
                    done();
                });
            });
        });
    });
});
