const assert = require('assert');
const { S3 } = require('aws-sdk');
const getConfig = require('../support/config');
const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const bucket = `bucketacl-bucket-${Date.now()}`;
const bucketName = 'putbucketaclfttest';
const grants = [];

// results in body of 589824 bytes
for (let i = 0; i < 100000; i++) {
    grants.push({
        Grantee: {
            Type: 'CanonicalUser',
            DisplayName: 'STRING_VALUE',
            EmailAddress: 'STRING_VALUE',
            ID: 'STRING_VALUE',
        },
        Permission: 'READ',
    });
}

describe('aws-node-sdk test bucket put acl', () => {
    let s3;

    // setup test
    before(done => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3(config);
        s3.createBucket({ Bucket: bucket }, done);
    });

    // delete bucket after testing
    after(done => s3.deleteBucket({ Bucket: bucket }, done));

    const itSkipIfAWS = process.env.AWS_ON_AIR ? it.skip : it;
    itSkipIfAWS('should not accept xml body larger than 512 KB', done => {
        const params = {
            Bucket: bucket,
            AccessControlPolicy: {
                Grants: grants,
                Owner: {
                    DisplayName: 'STRING_VALUE',
                    ID: 'STRING_VALUE',
                },
            },
        };
        s3.putBucketAcl(params, error => {
            if (error) {
                assert.strictEqual(error.statusCode, 400);
                assert.strictEqual(
                    error.code, 'InvalidRequest');
                done();
            } else {
                done('accepted xml body larger than 512 KB');
            }
        });
    });
});

describe('PUT Bucket ACL', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;

        beforeEach(() => {
            process.stdout.write('About to create bucket');
            return bucketUtil.createOne(bucketName).catch(err => {
                process.stdout.write(`Error in beforeEach ${err}\n`);
                throw err;
            });
        });

        afterEach(() => {
            process.stdout.write('About to delete bucket');
            return bucketUtil.deleteOne(bucketName).catch(err => {
                process.stdout.write(`Error in afterEach ${err}\n`);
                throw err;
            });
        });

        it('should return InvalidArgument if invalid grantee ' +
            'user ID provided in ACL header request', done => {
            s3.putBucketAcl({
                Bucket: bucketName,
                GrantRead: 'id=invalidUserID' }, err => {
                assert.strictEqual(err.statusCode, 400);
                assert.strictEqual(err.code, 'InvalidArgument');
                done();
            });
        });

        it('should return InvalidArgument if invalid grantee ' +
            'user ID provided in ACL request body', done => {
            s3.putBucketAcl({
                Bucket: bucketName,
                AccessControlPolicy: {
                    Grants: [
                        {
                            Grantee: {
                                Type: 'CanonicalUser',
                                ID: 'invalidUserID',
                            },
                            Permission: 'WRITE_ACP',
                        }],
                    Owner: {
                        DisplayName: 'Bart',
                        ID: '79a59df900b949e55d96a1e698fbace' +
                        'dfd6e09d98eacf8f8d5218e7cd47ef2be',
                    },
                },
            }, err => {
                assert.strictEqual(err.statusCode, 400);
                assert.strictEqual(err.code, 'InvalidArgument');
                done();
            });
        });
    });
});
