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
const { GetObjectAttributesExtendedCommand } = require('@scality/cloudserverclient');
const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const { algorithms } = require('../../../../../lib/api/apiUtils/integrity/validateChecksums');

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
            await s3.send(
                new PutObjectCommand({
                    Bucket: bucket,
                    Key: key,
                    Body: body,
                    ChecksumAlgorithm: 'CRC64NVME',
                }),
            );
        });

        afterEach(async () => {
            await s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: key }));
            await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
        });

        it('should fail with a wrong bucket owner header', async () => {
            try {
                await s3.send(
                    new GetObjectAttributesCommand({
                        Bucket: bucket,
                        Key: key,
                        ObjectAttributes: ['ETag'],
                        ExpectedBucketOwner: 'wrongAccountId',
                    }),
                );
                assert.fail('Expected AccessDenied error');
            } catch (err) {
                assert.strictEqual(err.name, 'AccessDenied');
                assert.strictEqual(err.message, 'Access Denied');
            }
        });

        it('should fail because attributes header is missing', async () => {
            try {
                await s3.send(
                    new GetObjectAttributesCommand({
                        Bucket: bucket,
                        Key: key,
                        ObjectAttributes: [],
                    }),
                );
                assert.fail('Expected InvalidArgument error');
            } catch (err) {
                assert.strictEqual(err.name, 'InvalidArgument');
                assert.strictEqual(err.message, 'Invalid attribute name specified.');
            }
        });

        it('should fail because attribute name is invalid', async () => {
            try {
                await s3.send(
                    new GetObjectAttributesCommand({
                        Bucket: bucket,
                        Key: key,
                        ObjectAttributes: ['InvalidAttribute'],
                    }),
                );
                assert.fail('Expected InvalidArgument error');
            } catch (err) {
                assert.strictEqual(err.name, 'InvalidArgument');
                assert.strictEqual(err.message, 'Invalid attribute name specified.');
            }
        });

        it('should return NoSuchKey for non-existent object', async () => {
            try {
                await s3.send(
                    new GetObjectAttributesCommand({
                        Bucket: bucket,
                        Key: 'nonexistent',
                        ObjectAttributes: ['ETag'],
                    }),
                );
                assert.fail('Expected NoSuchKey error');
            } catch (err) {
                assert.strictEqual(err.name, 'NoSuchKey');
                assert.strictEqual(err.message, 'The specified key does not exist.');
            }
        });

        it('should return all attributes', async () => {
            const data = await s3.send(
                new GetObjectAttributesCommand({
                    Bucket: bucket,
                    Key: key,
                    ObjectAttributes: ['ETag', 'ObjectParts', 'StorageClass', 'ObjectSize'],
                }),
            );

            assert.strictEqual(data.ETag, expectedMD5);
            assert.strictEqual(data.StorageClass, 'STANDARD');
            assert.strictEqual(data.ObjectSize, body.length);
            assert.strictEqual(data.ObjectParts, undefined, "ObjectParts shouldn't be present for non-MPU object");
            assert(data.LastModified, 'LastModified should be present');
        });

        it('should return ETag', async () => {
            const data = await s3.send(
                new GetObjectAttributesCommand({
                    Bucket: bucket,
                    Key: key,
                    ObjectAttributes: ['ETag'],
                }),
            );

            assert.strictEqual(data.ETag, expectedMD5);
        });

        it('should return ChecksumCRC64NVME for object', async () => {
            const data = await s3.send(
                new GetObjectAttributesCommand({
                    Bucket: bucket,
                    Key: key,
                    ObjectAttributes: ['Checksum'],
                }),
            );

            assert(data.Checksum, 'Checksum should be present');
            assert(data.Checksum.ChecksumCRC64NVME, 'ChecksumCRC64NVME should be present');
            assert.strictEqual(data.Checksum.ChecksumType, 'FULL_OBJECT');
        });

        it('should not return Checksum when not requested', async () => {
            const data = await s3.send(
                new GetObjectAttributesCommand({
                    Bucket: bucket,
                    Key: key,
                    ObjectAttributes: ['ETag', 'ObjectSize'],
                }),
            );

            assert(data.ETag, 'ETag should be present');
            assert(data.ObjectSize, 'ObjectSize should be present');
            assert.strictEqual(data.Checksum, undefined, 'Checksum should not be present');
        });

        it("shouldn't return ObjectParts for non-MPU objects", async () => {
            // Requesting only ObjectParts for a non-MPU object break AWS SDK v3
            const data = await s3.send(
                new GetObjectAttributesCommand({
                    Bucket: bucket,
                    Key: key,
                    ObjectAttributes: ['ObjectParts', 'ETag'],
                }),
            );

            assert.strictEqual(data.ObjectParts, undefined, "ObjectParts shouldn't be present");
            assert.strictEqual(data.ETag, expectedMD5);
        });

        it('should return StorageClass', async () => {
            const data = await s3.send(
                new GetObjectAttributesCommand({
                    Bucket: bucket,
                    Key: key,
                    ObjectAttributes: ['StorageClass'],
                }),
            );

            assert.strictEqual(data.StorageClass, 'STANDARD');
        });

        it('should return ObjectSize', async () => {
            const data = await s3.send(
                new GetObjectAttributesCommand({
                    Bucket: bucket,
                    Key: key,
                    ObjectAttributes: ['ObjectSize'],
                }),
            );

            assert.strictEqual(data.ObjectSize, body.length);
        });

        it('should return LastModified', async () => {
            const data = await s3.send(
                new GetObjectAttributesCommand({
                    Bucket: bucket,
                    Key: key,
                    ObjectAttributes: ['ETag'],
                }),
            );

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

            const createResult = await s3.send(
                new CreateMultipartUploadCommand({
                    Bucket: bucket,
                    Key: mpuKey,
                }),
            );
            const uploadId = createResult.UploadId;

            const partData = Buffer.alloc(partSize, 'a');
            const parts = [];
            for (let i = 1; i <= partCount; i++) {
                const uploadResult = await s3.send(
                    new UploadPartCommand({
                        Bucket: bucket,
                        Key: mpuKey,
                        PartNumber: i,
                        UploadId: uploadId,
                        Body: partData,
                    }),
                );
                parts.push({ PartNumber: i, ETag: uploadResult.ETag });
            }

            await s3.send(
                new CompleteMultipartUploadCommand({
                    Bucket: bucket,
                    Key: mpuKey,
                    UploadId: uploadId,
                    MultipartUpload: { Parts: parts },
                }),
            );
        });

        after(async () => {
            await s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: mpuKey }));
            await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
        });

        it('should return TotalPartsCount for MPU object', async () => {
            const data = await s3.send(
                new GetObjectAttributesCommand({
                    Bucket: bucket,
                    Key: mpuKey,
                    ObjectAttributes: ['ObjectParts'],
                }),
            );

            assert(data.ObjectParts, 'ObjectParts should be present');
            assert.strictEqual(data.ObjectParts.TotalPartsCount, partCount);
        });

        it('should return TotalPartsCount along with other attributes for MPU object', async () => {
            const data = await s3.send(
                new GetObjectAttributesCommand({
                    Bucket: bucket,
                    Key: mpuKey,
                    ObjectAttributes: ['ETag', 'ObjectParts', 'ObjectSize', 'StorageClass'],
                }),
            );

            assert(data.ETag, 'ETag should be present');
            assert(data.ETag.includes(`-${partCount}`), `ETag should indicate MPU with ${partCount} parts`);
            assert(data.ObjectParts, 'ObjectParts should be present');
            assert.strictEqual(data.ObjectParts.TotalPartsCount, partCount);
            assert.strictEqual(data.ObjectSize, partSize * partCount);
            assert.strictEqual(data.StorageClass, 'STANDARD');
        });
    });
});

describe('objectGetAttributes with user metadata', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        before(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
        });

        beforeEach(async () => {
            await s3.send(new CreateBucketCommand({ Bucket: bucket }));
        });

        afterEach(async () => {
            await s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: key }));
            await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
        });

        it('should return specific user metadata when requested', async () => {
            await s3.send(
                new PutObjectCommand({
                    Bucket: bucket,
                    Key: key,
                    Body: body,
                    Metadata: {
                        'custom-key': 'custom-value',
                        'another-key': 'another-value',
                    },
                }),
            );

            const response = await s3.send(
                new GetObjectAttributesExtendedCommand({
                    Bucket: bucket,
                    Key: key,
                    ObjectAttributes: ['x-amz-meta-custom-key'],
                }),
            );

            assert.strictEqual(response['x-amz-meta-custom-key'], 'custom-value');
        });

        it('should return multiple user metadata when requested', async () => {
            await s3.send(
                new PutObjectCommand({
                    Bucket: bucket,
                    Key: key,
                    Body: body,
                    Metadata: {
                        foo: 'foo-value',
                        bar: 'bar-value',
                        baz: 'baz-value',
                    },
                }),
            );

            const response = await s3.send(
                new GetObjectAttributesExtendedCommand({
                    Bucket: bucket,
                    Key: key,
                    ObjectAttributes: ['x-amz-meta-foo', 'x-amz-meta-bar'],
                }),
            );

            assert.strictEqual(response['x-amz-meta-foo'], 'foo-value');
            assert.strictEqual(response['x-amz-meta-bar'], 'bar-value');
        });

        it('should return only all user metadata when x-amz-meta-* is requested', async () => {
            await s3.send(
                new PutObjectCommand({
                    Bucket: bucket,
                    Key: key,
                    Body: body,
                    Metadata: {
                        key1: 'value1',
                        key2: 'value2',
                        key3: 'value3',
                    },
                }),
            );

            const response = await s3.send(
                new GetObjectAttributesExtendedCommand({
                    Bucket: bucket,
                    Key: key,
                    ObjectAttributes: ['x-amz-meta-*'],
                }),
            );

            assert.strictEqual(response['x-amz-meta-key1'], 'value1');
            assert.strictEqual(response['x-amz-meta-key2'], 'value2');
            assert.strictEqual(response['x-amz-meta-key3'], 'value3');
            assert.strictEqual(response['x-amz-meta-*'], undefined, 'wildcard marker should not be in response');
        });

        it('should return empty response when object has no user metadata and x-amz-meta-* is requested', async () => {
            await s3.send(
                new PutObjectCommand({
                    Bucket: bucket,
                    Key: key,
                    Body: body,
                }),
            );

            const response = await s3.send(
                new GetObjectAttributesExtendedCommand({
                    Bucket: bucket,
                    Key: key,
                    ObjectAttributes: ['ETag', 'x-amz-meta-*'],
                }),
            );

            const metadataKeys = Object.keys(response).filter(k => k.startsWith('x-amz-meta-'));
            assert.strictEqual(metadataKeys.length, 0);
        });

        it('should return empty response when requested metadata key does not exist', async () => {
            await s3.send(
                new PutObjectCommand({
                    Bucket: bucket,
                    Key: key,
                    Body: body,
                    Metadata: {
                        existing: 'value',
                    },
                }),
            );

            const response = await s3.send(
                new GetObjectAttributesExtendedCommand({
                    Bucket: bucket,
                    Key: key,
                    ObjectAttributes: ['ETag', 'x-amz-meta-nonexistent'],
                }),
            );

            assert.strictEqual(response['x-amz-meta-nonexistent'], undefined);
        });

        it('should return empty response when only a non-existing metadata key is requested', async () => {
            await s3.send(
                new PutObjectCommand({
                    Bucket: bucket,
                    Key: key,
                    Body: body,
                    Metadata: {
                        existing: 'value',
                    },
                }),
            );

            const response = await s3.send(
                new GetObjectAttributesExtendedCommand({
                    Bucket: bucket,
                    Key: key,
                    ObjectAttributes: ['x-amz-meta-nonexistent'],
                }),
            );

            assert.strictEqual(response['x-amz-meta-nonexistent'], undefined);
        });

        it('should return user metadata along with standard attributes', async () => {
            await s3.send(
                new PutObjectCommand({
                    Bucket: bucket,
                    Key: key,
                    Body: body,
                    Metadata: {
                        custom: 'custom-value',
                    },
                }),
            );

            const response = await s3.send(
                new GetObjectAttributesExtendedCommand({
                    Bucket: bucket,
                    Key: key,
                    ObjectAttributes: ['ETag', 'x-amz-meta-custom', 'ObjectSize'],
                }),
            );

            assert.strictEqual(response.ETag, expectedMD5);
            assert.strictEqual(response.ObjectSize, body.length);
            assert.strictEqual(response['x-amz-meta-custom'], 'custom-value');
        });

        it('should return all metadata once wildcard is provided', async () => {
            await s3.send(
                new PutObjectCommand({
                    Bucket: bucket,
                    Key: key,
                    Body: body,
                    Metadata: {
                        key1: 'value1',
                        key2: 'value2',
                        key3: 'value3',
                    },
                }),
            );

            const response = await s3.send(
                new GetObjectAttributesExtendedCommand({
                    Bucket: bucket,
                    Key: key,
                    ObjectAttributes: ['x-amz-meta-*', 'x-amz-meta-key1'],
                }),
            );

            assert.strictEqual(response['x-amz-meta-key1'], 'value1');
            assert.strictEqual(response['x-amz-meta-key2'], 'value2');
            assert.strictEqual(response['x-amz-meta-key3'], 'value3');
        });

        it('should handle duplicate wildcard requests without duplicating results', async () => {
            await s3.send(
                new PutObjectCommand({
                    Bucket: bucket,
                    Key: key,
                    Body: body,
                    Metadata: {
                        key1: 'value1',
                        key2: 'value2',
                    },
                }),
            );

            const response = await s3.send(
                new GetObjectAttributesExtendedCommand({
                    Bucket: bucket,
                    Key: key,
                    ObjectAttributes: ['x-amz-meta-*', 'x-amz-meta-*'],
                }),
            );

            assert.strictEqual(response['x-amz-meta-key1'], 'value1');
            assert.strictEqual(response['x-amz-meta-key2'], 'value2');
        });

        it('should handle duplicate specific metadata requests without duplicating results', async () => {
            await s3.send(
                new PutObjectCommand({
                    Bucket: bucket,
                    Key: key,
                    Body: body,
                    Metadata: {
                        key1: 'value1',
                        key2: 'value2',
                    },
                }),
            );

            const response = await s3.send(
                new GetObjectAttributesExtendedCommand({
                    Bucket: bucket,
                    Key: key,
                    ObjectAttributes: ['x-amz-meta-key1', 'x-amz-meta-key1'],
                }),
            );

            assert.strictEqual(response['x-amz-meta-key1'], 'value1');
            assert.strictEqual(response['x-amz-meta-key2'], undefined);
        });
    });
});

describe('objectGetAttributes with checksum', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;
        const checksumBucket = 'checksum-getattr-test';
        const checksumKey = 'checksum-test-object';
        const checksumBody = Buffer.from('checksum test body');

        const expectedDigests = {};

        before(async () => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            await s3.send(new CreateBucketCommand({ Bucket: checksumBucket }));

            for (const [name, algo] of Object.entries(algorithms)) {
                expectedDigests[name] = await algo.digest(checksumBody);
            }
        });

        after(async () => {
            await bucketUtil.empty(checksumBucket);
            await s3.send(new DeleteBucketCommand({ Bucket: checksumBucket }));
        });

        Object.entries(algorithms).forEach(([name, { getObjectAttributesXMLTag }]) => {
            const sdkAlgorithm = name.toUpperCase();

            it(`should return ${getObjectAttributesXMLTag} when object has ${name} checksum`, async () => {
                await s3.send(
                    new PutObjectCommand({
                        Bucket: checksumBucket,
                        Key: checksumKey,
                        Body: checksumBody,
                        ChecksumAlgorithm: sdkAlgorithm,
                    }),
                );

                const data = await s3.send(
                    new GetObjectAttributesCommand({
                        Bucket: checksumBucket,
                        Key: checksumKey,
                        ObjectAttributes: ['Checksum'],
                    }),
                );

                assert(data.Checksum, 'Checksum should be present');
                assert.strictEqual(data.Checksum[getObjectAttributesXMLTag], expectedDigests[name]);
                assert.strictEqual(data.Checksum.ChecksumType, 'FULL_OBJECT');
            });

            it(`should return ${getObjectAttributesXMLTag} along with other attributes`, async () => {
                await s3.send(
                    new PutObjectCommand({
                        Bucket: checksumBucket,
                        Key: checksumKey,
                        Body: checksumBody,
                        ChecksumAlgorithm: sdkAlgorithm,
                    }),
                );

                const data = await s3.send(
                    new GetObjectAttributesCommand({
                        Bucket: checksumBucket,
                        Key: checksumKey,
                        ObjectAttributes: ['ETag', 'Checksum', 'ObjectSize'],
                    }),
                );

                assert(data.ETag, 'ETag should be present');
                assert(data.ObjectSize, 'ObjectSize should be present');
                assert(data.Checksum, 'Checksum should be present');
                assert.strictEqual(data.Checksum[getObjectAttributesXMLTag], expectedDigests[name]);
                assert.strictEqual(data.Checksum.ChecksumType, 'FULL_OBJECT');
            });
        });

        it('should not return Checksum when not requested', async () => {
            await s3.send(
                new PutObjectCommand({
                    Bucket: checksumBucket,
                    Key: checksumKey,
                    Body: checksumBody,
                    ChecksumAlgorithm: 'CRC64NVME',
                }),
            );

            const data = await s3.send(
                new GetObjectAttributesCommand({
                    Bucket: checksumBucket,
                    Key: checksumKey,
                    ObjectAttributes: ['ETag', 'ObjectSize'],
                }),
            );

            assert(data.ETag, 'ETag should be present');
            assert(data.ObjectSize, 'ObjectSize should be present');
            assert.strictEqual(data.Checksum, undefined, 'Checksum should not be present');
        });
    });
});
