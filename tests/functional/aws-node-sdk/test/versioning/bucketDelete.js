const assert = require('assert');
const {
    CreateBucketCommand,
    PutBucketVersioningCommand,
    DeleteBucketCommand,
    PutObjectCommand,
    DeleteObjectCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const { removeAllVersions } = require('../../lib/utility/versioning-util.js');

const bucketName = `versioning-bucket-${Date.now()}`;
const key = 'anObject';


function checkError(err, code) {
    assert.notEqual(err, null, 'Expected failure but got success');
    assert.strictEqual(err.Code, code);
}

describe('aws-node-sdk test delete bucket', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;

        // setup test
        beforeEach(async () => {
            await s3.send(new CreateBucketCommand({ Bucket: bucketName }));
            await s3.send(new PutBucketVersioningCommand({
                Bucket: bucketName,
                VersioningConfiguration: {
                    Status: 'Enabled',
                },
            }));
        });

        // empty and delete bucket after testing if bucket exists
         afterEach(done => {
            removeAllVersions({ Bucket: bucketName }, err => {
                if (err?.name === 'NoSuchBucket') {
                    return done();
                }
                return s3.send(new DeleteBucketCommand({ Bucket: bucketName }))
                .then(() => done()).catch(err => {
                    if (err.name === 'NoSuchBucket') {
                        return done();
                    }
                    return done(err);
                });
            });
        });

        it('should be able to delete empty bucket with version enabled',
        async () => {
            await s3.send(new DeleteBucketCommand({ Bucket: bucketName }));
        });

        it('should return error 409 BucketNotEmpty if trying to delete bucket' +
        ' containing delete marker', async () => {
            await s3.send(new DeleteObjectCommand({ Bucket: bucketName, Key: key }));
            
            try {
                await s3.send(new DeleteBucketCommand({ Bucket: bucketName }));
                assert.fail('Expected BucketNotEmpty error but got success');
            } catch (err) {
                checkError(err, 'BucketNotEmpty');
            }
        });

        it('should return error 409 BucketNotEmpty if trying to delete bucket' +
        ' containing version and delete marker', async () => {
            await s3.send(new PutObjectCommand({ Bucket: bucketName, Key: key }));
            await s3.send(new DeleteObjectCommand({ Bucket: bucketName, Key: key }));
            
            try {
                await s3.send(new DeleteBucketCommand({ Bucket: bucketName }));
                assert.fail('Expected BucketNotEmpty error but got success');
            } catch (err) {
                checkError(err, 'BucketNotEmpty');
            }
        });

        it('should return error 404 NoSuchBucket if the bucket name is invalid',
        async () => {
            try {
                await s3.send(new DeleteBucketCommand({ Bucket: 'bucketA' }));
                assert.fail('Expected NoSuchBucket error but got success');
            } catch (err) {
                checkError(err, 'NoSuchBucket');
            }
        });
    });
});
