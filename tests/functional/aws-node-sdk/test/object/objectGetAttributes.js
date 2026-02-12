const assert = require('assert');
const {
    CreateBucketCommand,
    DeleteBucketCommand,
    DeleteObjectCommand,
    PutObjectCommand,
    GetObjectAttributesCommand,
    CreateMultipartUploadCommand,
    UploadPartCommand,
    CompleteMultipartUploadCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const bucket = 'testbucket';
const key = 'testobject';
const body = 'hello world!';
const expectedMD5 = 'fc3ff98e8c6a0d3087d515c0473f8677';

describe('objectGetAttributes', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        before(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
        });

        beforeEach(async () => {
            await s3.send(new CreateBucketCommand({ Bucket: bucket }));
            await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: body }));
        });

        afterEach(async () => {
            await s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: key }));
            await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
        });

        it('should fail with a wrong bucket owner header', async () => {
            try {
                await s3.send(new GetObjectAttributesCommand({
                    Bucket: bucket,
                    Key: key,
                    ObjectAttributes: ['ETag'],
                    ExpectedBucketOwner: 'wrongAccountId',
                }));
                assert.fail('Expected AccessDenied error');
            } catch (err) {
                assert.strictEqual(err.name, 'AccessDenied');
                assert.strictEqual(err.message, 'Access Denied');
            }
        });

        it('should fail because attributes header is missing', async () => {
            try {
                await s3.send(new GetObjectAttributesCommand({
                    Bucket: bucket,
                    Key: key,
                    ObjectAttributes: [],
                }));
                assert.fail('Expected InvalidArgument error');
            } catch (err) {
                assert.strictEqual(err.name, 'InvalidArgument');
                assert.strictEqual(err.message, 'Invalid attribute name specified.');
            }
        });

        it('should fail because attribute name is invalid', async () => {
            try {
                await s3.send(new GetObjectAttributesCommand({
                    Bucket: bucket,
                    Key: key,
                    ObjectAttributes: ['InvalidAttribute'],
                }));
                assert.fail('Expected InvalidArgument error');
            } catch (err) {
                assert.strictEqual(err.name, 'InvalidArgument');
                assert.strictEqual(err.message, 'Invalid attribute name specified.');
            }
        });

        it('should return NoSuchKey for non-existent object', async () => {
            try {
                await s3.send(new GetObjectAttributesCommand({
                    Bucket: bucket,
                    Key: 'nonexistent',
                    ObjectAttributes: ['ETag'],
                }));
                assert.fail('Expected NoSuchKey error');
            } catch (err) {
                assert.strictEqual(err.name, 'NoSuchKey');
                assert.strictEqual(err.message, 'The specified key does not exist.');
            }
        });

        it('should return all attributes', async () => {
            const data = await s3.send(new GetObjectAttributesCommand({
                Bucket: bucket,
                Key: key,
                ObjectAttributes: ['ETag', 'ObjectParts', 'StorageClass', 'ObjectSize'],
            }));

            assert.strictEqual(data.ETag, expectedMD5);
            assert.strictEqual(data.StorageClass, 'STANDARD');
            assert.strictEqual(data.ObjectSize, body.length);
            assert.strictEqual(data.ObjectParts, undefined, "ObjectParts shouldn't be present for non-MPU object");
            assert(data.LastModified, 'LastModified should be present');
        });

        it('should return ETag', async () => {
            const data = await s3.send(new GetObjectAttributesCommand({
                Bucket: bucket,
                Key: key,
                ObjectAttributes: ['ETag'],
            }));

            assert.strictEqual(data.ETag, expectedMD5);
        });

        it('should fail with NotImplemented when Checksum is requested', async () => {
            try {
                await s3.send(new GetObjectAttributesCommand({
                    Bucket: bucket,
                    Key: key,
                    ObjectAttributes: ['Checksum'],
                }));
                assert.fail('Expected NotImplemented error');
            } catch (err) {
                assert.strictEqual(err.name, 'NotImplemented');
                assert.strictEqual(err.message, 'Checksum attribute is not implemented');
            }
        });

        it("shouldn't return ObjectParts for non-MPU objects", async () => {
            // Requesting only ObjectParts for a non-MPU object break AWS SDK v3
            const data = await s3.send(new GetObjectAttributesCommand({
                Bucket: bucket,
                Key: key,
                ObjectAttributes: ['ObjectParts', 'ETag'],
            }));

            assert.strictEqual(data.ObjectParts, undefined, "ObjectParts shouldn't be present");
            assert.strictEqual(data.ETag, expectedMD5);
        });

        it('should return StorageClass', async () => {
            const data = await s3.send(new GetObjectAttributesCommand({
                Bucket: bucket,
                Key: key,
                ObjectAttributes: ['StorageClass'],
            }));

            assert.strictEqual(data.StorageClass, 'STANDARD');
        });

        it('should return ObjectSize', async () => {
            const data = await s3.send(new GetObjectAttributesCommand({
                Bucket: bucket,
                Key: key,
                ObjectAttributes: ['ObjectSize'],
            }));

            assert.strictEqual(data.ObjectSize, body.length);
        });

        it('should return LastModified', async () => {
            const data = await s3.send(new GetObjectAttributesCommand({
                Bucket: bucket,
                Key: key,
                ObjectAttributes: ['ETag'],
            }));

            assert(data.LastModified, 'LastModified should be present');
            assert(data.LastModified instanceof Date, 'LastModified should be a Date');
            assert(!isNaN(data.LastModified.getTime()), 'LastModified should be a valid date');
        });
    });
});

describe('Test get object attributes with multipart upload', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;
        const mpuKey = 'mpuObject';
        const partSize = 5 * 1024 * 1024; // Minimum part size is 5MB
        const partCount = 3;

        before(async () => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;

            await s3.send(new CreateBucketCommand({ Bucket: bucket }));

            const createResult = await s3.send(new CreateMultipartUploadCommand({
                Bucket: bucket,
                Key: mpuKey,
            }));
            const uploadId = createResult.UploadId;

            const partData = Buffer.alloc(partSize, 'a');
            const parts = [];
            for (let i = 1; i <= partCount; i++) {
                const uploadResult = await s3.send(new UploadPartCommand({
                    Bucket: bucket,
                    Key: mpuKey,
                    PartNumber: i,
                    UploadId: uploadId,
                    Body: partData,
                }));
                parts.push({ PartNumber: i, ETag: uploadResult.ETag });
            }

            await s3.send(new CompleteMultipartUploadCommand({
                Bucket: bucket,
                Key: mpuKey,
                UploadId: uploadId,
                MultipartUpload: { Parts: parts },
            }));
        });

        after(async () => {
            await s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: mpuKey }));
            await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
        });

        it('should return TotalPartsCount for MPU object', async () => {
            const data = await s3.send(new GetObjectAttributesCommand({
                Bucket: bucket,
                Key: mpuKey,
                ObjectAttributes: ['ObjectParts'],
            }));

            assert(data.ObjectParts, 'ObjectParts should be present');
            assert.strictEqual(data.ObjectParts.TotalPartsCount, partCount);
        });

        it('should return TotalPartsCount along with other attributes for MPU object', async () => {
            const data = await s3.send(new GetObjectAttributesCommand({
                Bucket: bucket,
                Key: mpuKey,
                ObjectAttributes: ['ETag', 'ObjectParts', 'ObjectSize', 'StorageClass'],
            }));

            assert(data.ETag, 'ETag should be present');
            assert(data.ETag.includes(`-${partCount}`), `ETag should indicate MPU with ${partCount} parts`);
            assert(data.ObjectParts, 'ObjectParts should be present');
            assert.strictEqual(data.ObjectParts.TotalPartsCount, partCount);
            assert.strictEqual(data.ObjectSize, partSize * partCount);
            assert.strictEqual(data.StorageClass, 'STANDARD');
        });
    });
});
