const assert = require('assert');
const {
    S3Client,
    CreateBucketCommand,
    DeleteBucketCommand,
    DeleteBucketWebsiteCommand,
    PutBucketWebsiteCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const getConfig = require('../support/config');
const { WebsiteConfigTester } = require('../../lib/utility/website-util');

const bucketName = 'testdeletewebsitebucket';

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
                    assert.strictEqual(err.name, 'NoSuchBucket');
                    assert.strictEqual(err.$metadata.httpStatusCode, 404);
                }
            });
        });

        describe('with existing bucket', () => {
            beforeEach(() => s3.send(new CreateBucketCommand({ Bucket: bucketName })));

            afterEach(() => s3.send(new DeleteBucketCommand({ Bucket: bucketName })));

            describe('without existing configuration', () => {
                it('should return a 204 response', async () => {
                    const res = await s3.send(new DeleteBucketWebsiteCommand({ Bucket: bucketName }));
                    const statusCode = res?.$metadata?.httpStatusCode;
                    assert.strictEqual(statusCode, 204, `Found unexpected statusCode ${statusCode}`);
                });
            });

            describe('with existing configuration', () => {
                beforeEach(() => {
                    const config = new WebsiteConfigTester('index.html');
                    return s3.send(
                        new PutBucketWebsiteCommand({
                            Bucket: bucketName,
                            WebsiteConfiguration: config,
                        }),
                    );
                });

                it('should delete bucket configuration successfully', () =>
                    s3.send(new DeleteBucketWebsiteCommand({ Bucket: bucketName })));

                it('should return AccessDenied if user is not bucket owner', async () => {
                    try {
                        await otherAccountS3.send(new DeleteBucketWebsiteCommand({ Bucket: bucketName }));
                        throw new Error('Expected AccessDenied error');
                    } catch (err) {
                        assert.strictEqual(err.name, 'AccessDenied');
                        assert.strictEqual(err.$metadata.httpStatusCode, 403);
                    }
                });
            });
        });
    });
});
