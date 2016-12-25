import assert from 'assert';

import withV4 from '../support/withV4';
import BucketUtility from '../../lib/utility/bucket-util';
import provideRawOutput from '../../lib/utility/provideRawOutput';

const bucket = 'bucket2putstuffin4324242';

describe('PUT object', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        beforeEach(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return s3.createBucketAsync({ Bucket: bucket })
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            });
        });

        afterEach(() => {
            process.stdout.write('Emptying bucket');
            return bucketUtil.empty(bucket)
            .then(() => {
                process.stdout.write('Deleting bucket');
                return bucketUtil.deleteOne(bucket);
            })
            .catch(err => {
                process.stdout.write('Error in afterEach');
                throw err;
            });
        });

        it('should put an object and set the acl via query param',
            done => {
                const params = { Bucket: bucket, Key: 'key',
                ACL: 'public-read', StorageClass: 'STANDARD' };
                const url = s3.getSignedUrl('putObject', params);
                provideRawOutput(['-verbose', '-X', 'PUT', url,
                '--upload-file', 'package.json'], httpCode => {
                    assert.strictEqual(httpCode, '200 OK');
                    s3.getObjectAcl({ Bucket: bucket, Key: 'key' },
                    (err, result) => {
                        assert.equal(err, null, 'Expected success, ' +
                        `got error ${JSON.stringify(err)}`);
                        assert.deepStrictEqual(result.Grants[1], { Grantee:
                            { Type: 'Group', URI:
                            'http://acs.amazonaws.com/groups/global/AllUsers',
                        }, Permission: 'READ' });
                        done();
                    });
                });
            });

        it('should return Not Implemented error for obj. encryption using ' +
            'AWS-managed encryption keys', done => {
            const params = { Bucket: bucket, Key: 'key',
                ServerSideEncryption: 'AES256' };
            s3.putObject(params, err => {
                assert.strictEqual(err.code, 'NotImplemented');
                done();
            });
        });

        it('should return Not Implemented error for obj. encryption using ' +
            'customer-provided encryption keys', done => {
            const params = { Bucket: bucket, Key: 'key',
                SSECustomerAlgorithm: 'AES256' };
            s3.putObject(params, err => {
                assert.strictEqual(err.code, 'NotImplemented');
                done();
            });
        });
    });
});
