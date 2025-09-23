const assert = require('assert');
const { S3Client,
    CreateBucketCommand,
    DeleteBucketCommand,
    DeleteBucketWebsiteCommand,
    PutBucketWebsiteCommand } = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const getConfig = require('../support/config');
const { WebsiteConfigTester } = require('../../lib/utility/website-util');

const bucketName = 'testdeletewebsitebucket';

// Helper function to delete bucket (replacing bucketUtil.deleteOne)
async function deleteBucket(s3, bucket) {
    try {
        await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
    } catch (err) {
        // eslint-disable-next-line no-console
        console.log(err);
    }
}

describe('DELETE bucket website', () => {
    withV4(sigCfg => {
        const config = getConfig('default', sigCfg);
        const s3 = new S3Client(config);
        const otherAccountConfig = getConfig('lisa', {});
        const otherAccountS3 = new S3Client(otherAccountConfig);

        describe('without existing bucket', () => {
            it('should return NoSuchBucket', async () => {
                try {
                    await s3.send(new DeleteBucketWebsiteCommand({ Bucket: bucketName }));
                    throw new Error('Expected NoSuchBucket error');
                } catch (err) {
                    assert(err);
                    assert.strictEqual(err.Code, 'NoSuchBucket');
                    assert.strictEqual(err.$metadata.httpStatusCode, 404);
                }
            });
        });

        describe('with existing bucket', () => {
            beforeEach(async () => {
                await s3.send(new CreateBucketCommand({ Bucket: bucketName }));
            });
            
            afterEach(() => deleteBucket(s3, bucketName));

            describe('without existing configuration', () => {
                it('should return a 204 response', async () => {
                    const res = await s3.send(new DeleteBucketWebsiteCommand({ Bucket: bucketName }));
                    const statusCode = res?.$metadata?.httpStatusCode;
                    assert.strictEqual(statusCode, 204,
                        `Found unexpected statusCode ${statusCode}`);
                });
            });

            describe('with existing configuration', () => {
                beforeEach(async () => {
                    const config = new WebsiteConfigTester('index.html');
                    await s3.send(new PutBucketWebsiteCommand({ 
                        Bucket: bucketName,
                        WebsiteConfiguration: config 
                    }));
                });

                it('should delete bucket configuration successfully', async () => {
                    await s3.send(new DeleteBucketWebsiteCommand({ Bucket: bucketName }));
                    // Should not throw an error
                });

                it('should return AccessDenied if user is not bucket owner', async () => {
                    try {
                        await otherAccountS3.send(new DeleteBucketWebsiteCommand({ Bucket: bucketName }));
                        throw new Error('Expected AccessDenied error');
                    } catch (err) {
                        assert(err);
                        assert.strictEqual(err.Code, 'AccessDenied');
                        assert.strictEqual(err.$metadata.httpStatusCode, 403);
                    }
                });
            });
        });
    });
});
