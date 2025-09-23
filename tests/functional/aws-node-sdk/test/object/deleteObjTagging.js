const assert = require('assert');
const {
    CreateBucketCommand,
    PutObjectCommand,
    PutObjectTaggingCommand,
    DeleteObjectTaggingCommand,
    PutObjectAclCommand,
    PutBucketAclCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const bucketName = 'testdeletetaggingbucket';
const objectName = 'testtaggingobject';
const objectNameAcl = 'testtaggingobjectacl';

const taggingConfig = { TagSet: [
    {
        Key: 'key1',
        Value: 'value1',
    },
    {
        Key: 'key2',
        Value: 'value2',
    },
] };

function _checkError(err, code, statusCode) {
    assert(err, 'Expected error but found none');
    assert.strictEqual(err.Code, code);
    assert.strictEqual(err.$metadata.httpStatusCode, statusCode);
}

describe('DELETE object taggings', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        const otherAccountBucketUtility = new BucketUtility('lisa', {});
        const otherAccountS3 = otherAccountBucketUtility.s3;

        beforeEach(async () => {
            await s3.send(new CreateBucketCommand({ Bucket: bucketName }));
            await s3.send(new PutObjectCommand({ Bucket: bucketName, Key: objectName }));
        });

        afterEach(async () => {
            process.stdout.write('Emptying bucket');
            await bucketUtil.empty(bucketName);
            process.stdout.write('Deleting bucket');
            await bucketUtil.deleteOne(bucketName);
        });

        it('should delete tag set', async () => {
            await s3.send(new PutObjectTaggingCommand({
                Bucket: bucketName,
                Key: objectName,
                Tagging: taggingConfig,
            }));
            
            const data = await s3.send(new DeleteObjectTaggingCommand({ 
                Bucket: bucketName, 
                Key: objectName 
            }));
            
            assert.strictEqual(Object.keys(data).length, 1); // Only $metadata should be present
        });

        it('should delete a non-existing tag set', async () => {
            const data = await s3.send(new DeleteObjectTaggingCommand({ 
                Bucket: bucketName, 
                Key: objectName 
            }));
            
            assert.strictEqual(Object.keys(data).length, 1); // Only $metadata should be present
        });

        it('should return NoSuchKey deleting tag set to a non-existing object', async () => {
            try {
                await s3.send(new DeleteObjectTaggingCommand({
                    Bucket: bucketName,
                    Key: 'nonexisting',
                }));
                assert.fail('Expected NoSuchKey error');
            } catch (err) {
                _checkError(err, 'NoSuchKey', 404);
            }
        });

        it('should return 403 AccessDenied deleting tag set with another account', async () => {
            try {
                await otherAccountS3.send(new DeleteObjectTaggingCommand({ 
                    Bucket: bucketName, 
                    Key: objectName 
                }));
                assert.fail('Expected AccessDenied error');
            } catch (err) {
                _checkError(err, 'AccessDenied', 403);
            }
        });

        it('should return 403 AccessDenied deleting tag set '+
            'with a different account to an object with ACL "public-read-write"', async () => {
            await s3.send(new PutObjectAclCommand({ 
                Bucket: bucketName, 
                Key: objectName,
                ACL: 'public-read-write' 
            }));
            
            try {
                await otherAccountS3.send(new DeleteObjectTaggingCommand({ 
                    Bucket: bucketName,
                    Key: objectName 
                }));
                assert.fail('Expected AccessDenied error');
            } catch (err) {
                _checkError(err, 'AccessDenied', 403);
            }
        });

        it('should return 403 AccessDenied deleting tag set to an object '+
            'in a bucket created with a different account', async () => {
            await s3.send(new PutBucketAclCommand({ 
                Bucket: bucketName, 
                ACL: 'public-read-write' 
            }));
            
            await otherAccountS3.send(new PutObjectCommand({ 
                Bucket: bucketName, 
                Key: objectNameAcl 
            }));
            
            try {
                await otherAccountS3.send(new DeleteObjectTaggingCommand({ 
                    Bucket: bucketName,
                    Key: objectNameAcl 
                }));
                assert.fail('Expected AccessDenied error');
            } catch (err) {
                _checkError(err, 'AccessDenied', 403);
            }
        });

        it('should delete tag set to an object in a bucket created with '+
            'same account even though object put by other account', async () => {
            await s3.send(new PutBucketAclCommand({ 
                Bucket: bucketName, 
                ACL: 'public-read-write' 
            }));
            
            await otherAccountS3.send(new PutObjectCommand({ 
                Bucket: bucketName, 
                Key: objectNameAcl 
            }));
            
            await s3.send(new DeleteObjectTaggingCommand({ 
                Bucket: bucketName,
                Key: objectNameAcl 
            }));
        });
    });
});
