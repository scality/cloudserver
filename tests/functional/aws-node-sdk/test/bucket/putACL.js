import assert from 'assert';

import withV4 from '../support/withV4';
import BucketUtility from '../../lib/utility/bucket-util';

const bucketName = 'putbucketaclfttest';

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
