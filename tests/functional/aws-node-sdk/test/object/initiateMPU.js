const assert = require('assert');
const {
    CreateBucketCommand,
    CreateMultipartUploadCommand,
    AbortMultipartUploadCommand,
    PutObjectCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const genMaxSizeMetaHeaders
    = require('../../lib/utility/genMaxSizeMetaHeaders');
const { generateMultipleTagQuery } = require('../../lib/utility/tagging');

const bucket = `initiatempubucket${Date.now()}`;
const key = 'key';

describe('Initiate MPU', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        beforeEach(async () => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            await s3.send(new CreateBucketCommand({ Bucket: bucket }));
        });

        afterEach(async () => {
            await bucketUtil.deleteOne(bucket);
        });

        it('should return InvalidRedirectLocation if initiate MPU ' +
        'with x-amz-website-redirect-location header that does not start ' +
        'with \'http://\', \'https://\' or \'/\'', async () => {
            const params = { 
                Bucket: bucket, 
                Key: key,
                WebsiteRedirectLocation: 'google.com' 
            };
            
            try {
                await s3.send(new CreateMultipartUploadCommand(params));
                throw new Error('Expected InvalidRedirectLocation error');
            } catch (err) {
                assert.strictEqual(err.name, 'InvalidRedirectLocation');
                assert.strictEqual(err.$metadata.httpStatusCode, 400);
            }
        });

        it('should return InvalidStorageClass error when x-amz-storage-class header is provided ' +
        'and not equal to STANDARD', async () => {
            try {
                await s3.send(new CreateMultipartUploadCommand({
                    Bucket: bucket,
                    Key: key,
                    StorageClass: 'COLD',
                }));
                throw new Error('Expected InvalidStorageClass error');
            } catch (err) {
                assert.strictEqual(err.name, 'InvalidStorageClass');
                assert.strictEqual(err.$metadata.httpStatusCode, 400);
            }
        });

        it('should return error if initiating MPU w/ > 2KB user-defined md',
        async () => {
            const metadata = genMaxSizeMetaHeaders();
            const params = { Bucket: bucket, Key: key, Metadata: metadata };
            
            // First, create an MPU with max size metadata (should succeed)
            const data = await s3.send(new CreateMultipartUploadCommand(params));
            const uploadId = data.UploadId;
            
            // Abort the upload
            await s3.send(new AbortMultipartUploadCommand({
                Bucket: bucket,
                Key: key,
                UploadId: uploadId,
            }));
            
            // Add one more byte to push over limit for next call
            metadata.header0 = `${metadata.header0}${'0'}`;
            
            // Now try to create another MPU with over-limit metadata (should fail)
            try {
                await s3.send(new CreateMultipartUploadCommand(params));
                throw new Error('Expected MetadataTooLarge error');
            } catch (err) {
                assert.strictEqual(err.name, 'MetadataTooLarge');
                assert.strictEqual(err.$metadata.httpStatusCode, 400);
            }
        });

        describe('with tag set', () => {
            it('should be able to put object with 10 tags',
            async () => {
                const taggingConfig = generateMultipleTagQuery(10);
                await s3.send(new CreateMultipartUploadCommand({
                    Bucket: bucket,
                    Key: key,
                    Tagging: taggingConfig,
                }));
            });

            it('should allow putting 50 tags', async () => {
                const taggingConfig = generateMultipleTagQuery(50);
                await s3.send(new CreateMultipartUploadCommand({
                    Bucket: bucket,
                    Key: key,
                    Tagging: taggingConfig,
                }));
            });

            it('should return BadRequest if putting more that 50 tags',
            async () => {
                const taggingConfig = generateMultipleTagQuery(51);
                
                try {
                    await s3.send(new CreateMultipartUploadCommand({
                        Bucket: bucket,
                        Key: key,
                        Tagging: taggingConfig,
                    }));
                    throw new Error('Expected BadRequest error');
                } catch (err) {
                    assert.strictEqual(err.name, 'BadRequest');
                    assert.strictEqual(err.$metadata.httpStatusCode, 400);
                }
            });

            it('should return InvalidArgument creating mpu tag with ' +
            'invalid characters: %', async () => {
                const value = 'value1%';
                
                try {
                    await s3.send(new CreateMultipartUploadCommand({
                        Bucket: bucket,
                        Key: key,
                        Tagging: `key1=${value}`,
                    }));
                    throw new Error('Expected InvalidArgument error');
                } catch (err) {
                    assert.strictEqual(err.name, 'InvalidArgument');
                    assert.strictEqual(err.$metadata.httpStatusCode, 400);
                }
            });

            it('should return InvalidArgument creating mpu with ' +
            'bad encoded tags', async () => {
                try {
                    await s3.send(new CreateMultipartUploadCommand({
                        Bucket: bucket,
                        Key: key,
                        Tagging: 'key1==value1',
                    }));
                    throw new Error('Expected InvalidArgument error');
                } catch (err) {
                    assert.strictEqual(err.name, 'InvalidArgument');
                    assert.strictEqual(err.$metadata.httpStatusCode, 400);
                }
            });

            it('should return InvalidArgument if tag with no key', async () => {
                try {
                    await s3.send(new CreateMultipartUploadCommand({
                        Bucket: bucket,
                        Key: key,
                        Tagging: '=value1',
                    }));
                    throw new Error('Expected InvalidArgument error');
                } catch (err) {
                    assert.strictEqual(err.name, 'InvalidArgument');
                    assert.strictEqual(err.$metadata.httpStatusCode, 400);
                }
            });

            it('should return InvalidArgument if using the same key twice',
            async () => {
                try {
                    await s3.send(new CreateMultipartUploadCommand({
                        Bucket: bucket,
                        Key: key,
                        Tagging: 'key1=value1&key1=value2',
                    }));
                    throw new Error('Expected InvalidArgument error');
                } catch (err) {
                    assert.strictEqual(err.name, 'InvalidArgument');
                    assert.strictEqual(err.$metadata.httpStatusCode, 400);
                }
            });

            it('should return InvalidArgument if using the same key twice ' +
            'and empty tags', async () => {
                try {
                    await s3.send(new PutObjectCommand({
                        Bucket: bucket,
                        Key: key,
                        Tagging: '&&&&&&&&&&&&&&&&&key1=value1&key1=value2',
                    }));
                    throw new Error('Expected InvalidArgument error');
                } catch (err) {
                    assert.strictEqual(err.name, 'InvalidArgument');
                    assert.strictEqual(err.$metadata.httpStatusCode, 400);
                }
            });
        });
    });
});
