const assert = require('assert');
const crypto = require('crypto');
const {
    S3Client,
    ListBucketsCommand,
    CreateBucketCommand,
    DeleteBucketCommand,
    CreateMultipartUploadCommand,
    UploadPartCommand,
    AbortMultipartUploadCommand,
    ListPartsCommand,
    CompleteMultipartUploadCommand,
    PutObjectCommand,
    GetObjectCommand,
    DeleteObjectCommand,
} = require('@aws-sdk/client-s3');

const getConfig = require('../support/config');
const { testsRangeOnEmptyFile } = require('../../../../unit/helpers');

const random = Math.round(Math.random() * 100).toString();
const bucket = `ftest-mybucket-${random}`;
const bucketEmptyObj = `ftest-bucketemptyobj-${random}`;

// Create a buffer to put as a multipart upload part
// and get its ETag
const md5HashFirstPart = crypto.createHash('md5');
const firstBufferBody = Buffer.alloc(5242880, 0);
const md5HashSecondPart = crypto.createHash('md5');
const secondBufferBody = Buffer.alloc(5242880, 1);
md5HashFirstPart.update(firstBufferBody);
md5HashSecondPart.update(secondBufferBody);
const calculatedFirstPartHash = md5HashFirstPart.digest('hex');
const calculatedSecondPartHash = md5HashSecondPart.digest('hex');
const combinedETag = '"0ea4f0f688a0be07ae1d92eb298d5218-2"';
const objectKey = 'toAbort&<>"\'';

// Store uploadId's in memory so can do multiple tests with
// same uploadId
const multipartUploadData = {};

describe('aws-node-sdk test suite as registered user', function testSuite() {
    this.timeout(60000);
    let s3;

    // setup test
    before(() => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3Client(config);
    });

    // bucketListing test
    it('should do bucket listing', async () => {
        const data = await s3.send(new ListBucketsCommand({}));
        assert(data.Buckets, 'No buckets Info sent back');
        assert(data.Owner, 'No owner Info sent back');
        assert(data.Owner.ID, 'Owner ID not sent back');
        assert(data.Owner.DisplayName, 'DisplayName not sent back');
        const owner = Object.keys(data.Owner);
        assert.strictEqual(owner.length, 2, 'Too much fields in owner');
    });

    // createbucket test
    it('should create a bucket', async () => {
        await s3.send(new CreateBucketCommand({ Bucket: bucket }));
    });

    // createMPU test
    it('should create a multipart upload', async () => {
        const data = await s3.send(new CreateMultipartUploadCommand({ Bucket: bucket, Key: objectKey }));
        assert.strictEqual(data.Bucket, bucket);
        assert.strictEqual(data.Key, objectKey);
        assert.ok(data.UploadId);
        multipartUploadData.firstUploadId = data.UploadId;
    });

    it('should upload a part of a multipart upload to be aborted', async () => {
        // uploadpart test
        const params = {
            Bucket: bucket,
            Key: objectKey,
            PartNumber: 1,
            UploadId: multipartUploadData.firstUploadId,
            Body: firstBufferBody,
        };
        const data = await s3.send(new UploadPartCommand(params));
        assert.strictEqual(data.ETag, `"${calculatedFirstPartHash}"`);
    });

    // abortMPU test
    it('should abort a multipart upload', async () => {
        const params = {
            Bucket: bucket,
            Key: objectKey,
            UploadId: multipartUploadData.firstUploadId,
        };
        const data = await s3.send(new AbortMultipartUploadCommand(params));
        assert.ok(data);
    });

    // createMPU test
    it('should upload a part of a multipart upload', async () => {
        const data = await s3.send(new CreateMultipartUploadCommand({ Bucket: bucket, Key: 'toComplete' }));
        const uploadId = data.UploadId;
        multipartUploadData.secondUploadId = data.UploadId;
        const params = {
            Bucket: bucket,
            Key: 'toComplete',
            PartNumber: 1,
            UploadId: uploadId,
            Body: firstBufferBody,
        };
        const uploadData = await s3.send(new UploadPartCommand(params));
        assert.strictEqual(uploadData.ETag, `"${calculatedFirstPartHash}"`);
    });

    it('should upload a second part of a multipart upload', async () => {
        // createMPU test
        const params = {
            Bucket: bucket,
            Key: 'toComplete',
            PartNumber: 2,
            UploadId: multipartUploadData.secondUploadId,
            Body: secondBufferBody,
        };
        const data = await s3.send(new UploadPartCommand(params));
        assert.strictEqual(data.ETag, `"${calculatedSecondPartHash}"`);
    });

    // listparts test
    it('should list the parts of a multipart upload', async () => {
        const params = {
            Bucket: bucket,
            Key: 'toComplete',
            UploadId: multipartUploadData.secondUploadId,
        };
        const data = await s3.send(new ListPartsCommand(params));
        assert.strictEqual(data.Bucket, bucket);
        assert.strictEqual(data.Key, 'toComplete');
        assert.strictEqual(data.UploadId, multipartUploadData.secondUploadId);
        assert.strictEqual(data.IsTruncated, false);
        assert.strictEqual(data.Parts[0].PartNumber, 1);
        assert.strictEqual(data.Parts[0].ETag, `"${calculatedFirstPartHash}"`);
        assert.strictEqual(data.Parts[0].Size, 5242880);
        assert.strictEqual(data.Parts[1].PartNumber, 2);
        assert.strictEqual(data.Parts[1].ETag, `"${calculatedSecondPartHash}"`);
        assert.strictEqual(data.Parts[1].Size, 5242880);
        // Must disable for now when running with Vault
        // since will need to pull actual ARN and canonicalId
        // assert.strictEqual(data.Initiator.ID, accessKey1ARN);
        // Note that for in memory implementation, "accessKey1"
        // is both the access key and the canonicalId so this
        // call works.  For real implementation with vault,
        // will need the canonicalId.
        // assert.strictEqual(data.Owner.ID, config.accessKeyId);
        assert.strictEqual(data.StorageClass, 'STANDARD');
    });

    it('should return an error if do not provide correct ' +
        // completempu test
        'xml when completing a multipart upload', async () => {
        const params = {
            Bucket: bucket,
            Key: 'toComplete',
            UploadId: multipartUploadData.secondUploadId,
        };
        try {
            await s3.send(new CompleteMultipartUploadCommand(params));
            throw new Error('Expected MalformedXML error');
        } catch (err) {
            assert.strictEqual(err.Code, 'MalformedXML');
        }
    });

    // completempu test
    it('should complete a multipart upload', async () => {
        const params = {
            Bucket: bucket,
            Key: 'toComplete',
            UploadId: multipartUploadData.secondUploadId,
            MultipartUpload: {
                Parts: [
                    {
                        ETag: calculatedFirstPartHash,
                        PartNumber: 1,
                    },
                    {
                        ETag: calculatedSecondPartHash,
                        PartNumber: 2,
                    },
                ],
            },
        };
        const data = await s3.send(new CompleteMultipartUploadCommand(params));
        assert.strictEqual(data.Bucket, bucket);
        assert.strictEqual(data.Key, 'toComplete');
        assert.strictEqual(data.ETag, combinedETag);
    });

    it('should get an object put by multipart upload', async () => {
        const params = {
            Bucket: bucket,
            Key: 'toComplete',
        };
        const data = await s3.send(new GetObjectCommand(params));
        assert.strictEqual(data.ETag, combinedETag);
        const uploadedObj = Buffer.concat([firstBufferBody, secondBufferBody]);
        const chunks = [];
        for await (const chunk of data.Body) {
            chunks.push(chunk);
        }
        const body = Buffer.concat(chunks);
        assert.deepStrictEqual(body, uploadedObj);
    });

    const mpuRangeGetTests = [
        { it: 'should get a range from the first part of an object ' +
            'put by multipart upload',
            range: 'bytes=0-9',
            contentLength: 10,
            contentRange: 'bytes 0-9/10485760',
            // Uploaded object is 5MB of 0 in the first part and
            // 5 MB of 1 in the second part so a range from the
            // first part should just contain 0
            expectedBuff: Buffer.alloc(10, 0),
        },
        { it: 'should get a range from the second part of an object ' +
            'put by multipart upload',
            // The completed MPU byte count starts at 0, so the first part ends
            // at byte 5242879 and the second part begins at byte 5242880
            range: 'bytes=5242880-5242889',
            contentLength: 10,
            contentRange: 'bytes 5242880-5242889/10485760',
            // A range from the second part should just contain 1
            expectedBuff: Buffer.alloc(10, 1),
        },
        { it: 'should get a range that spans both parts of an object put ' +
            'by multipart upload',
            range: 'bytes=5242875-5242884',
            contentLength: 10,
            contentRange: 'bytes 5242875-5242884/10485760',
            // Range that spans the two parts should contain 5 bytes
            // of 0 and 5 bytes of 1
            expectedBuff: Buffer.allocUnsafe(10).fill(0, 0, 5).fill(1, 5, 10),
        },
        { it: 'should get a range from the second part of an object put by ' +
            'multipart upload and include the end even if the range ' +
            'requested goes beyond the actual object end',
            // End is actually 10485759 since size is 10485760
            range: 'bytes=10485750-10485790',
            contentLength: 10,
            contentRange: 'bytes 10485750-10485759/10485760',
            // Range from the second part should just contain 1
            expectedBuff: Buffer.alloc(10, 1),
        },
        {
            it: 'should get entire object if range is invalid',
            range: 'bytes=-10485761',
            contentLength: 10485760,
            contentRange: 'bytes 0-10485759/10485760',
            expectedBuff: Buffer.concat([firstBufferBody, secondBufferBody]),
        },
    ];

    mpuRangeGetTests.forEach(test => {
        it(test.it, async () => {
            const params = {
                Bucket: bucket,
                Key: 'toComplete',
                Range: test.range,
            };
            const data = await s3.send(new GetObjectCommand(params));
            assert.strictEqual(data.ContentLength, test.contentLength);
            assert.strictEqual(data.AcceptRanges, 'bytes');
            assert.strictEqual(data.ContentRange, test.contentRange);
            assert.strictEqual(data.ETag, combinedETag);
            const chunks = [];
            for await (const chunk of data.Body) {
                chunks.push(chunk);
            }
            const body = Buffer.concat(chunks);
            assert.deepStrictEqual(body, test.expectedBuff);
        });
    });

    it('should delete object created by multipart upload', async () => {
        // deleteObject test
        const params = {
            Bucket: bucket,
            Key: 'toComplete',
        };
        const data = await s3.send(new DeleteObjectCommand(params));
        assert.ok(data);
    });

    it('should put an object regularly (non-MPU)', async () => {
        const params = {
            Bucket: bucket,
            Key: 'normalput',
            Body: Buffer.allocUnsafe(200).fill(0, 0, 50).fill(1, 50),
        };
        const data = await s3.send(new PutObjectCommand(params));
        assert.ok(data);
    });

    it('should return InvalidRange if the range of the resource does ' +
    'not cover the byte range', async () => {
        const params = {
            Bucket: bucket,
            Key: 'normalput',
            Range: 'bytes=200-200',
        };
        try {
            await s3.send(new GetObjectCommand(params));
            throw new Error('Expected InvalidRange error');
        } catch (err) {
            assert.strictEqual(err.Code, 'InvalidRange');
        }
    });

    describe('Get range on empty object', () => {
        const params = {
            Bucket: bucketEmptyObj,
            Key: 'emptyobj',
        };
        beforeEach(async () => {
            await s3.send(new CreateBucketCommand({ Bucket: bucketEmptyObj }));
            await s3.send(new PutObjectCommand(params));
        });
        afterEach(async () => {
            await s3.send(new DeleteObjectCommand(params));
            await s3.send(new DeleteBucketCommand({ Bucket: bucketEmptyObj }));
        });
        testsRangeOnEmptyFile.forEach(test => {
            const validText = test.valid ? 'InvalidRange error' : 'empty file';
            it(`should return ${validText} if get range ${test.range} on ` +
            'empty object', async () => {
                const getParams = {
                    Bucket: bucketEmptyObj,
                    Key: 'emptyobj',
                    Range: test.range,
                };
                try {
                    const data = await s3.send(new GetObjectCommand(getParams));
                    if (test.valid) {
                        throw new Error('Expected failure but got success');
                    }
                    const chunks = [];
                    for await (const chunk of data.Body) {
                        chunks.push(chunk);
                    }
                    const body = Buffer.concat(chunks);
                    assert.strictEqual(body.toString(), '');
                } catch (err) {
                    if (test.valid) {
                        assert.strictEqual(err.Code, 'InvalidRange');
                    } else {
                        throw err;
                    }
                }
            });
        });
    });

    const regularObjectRangeGetTests = [
        { it: 'should get a range for an object put without MPU',
            range: 'bytes=10-99',
            contentLength: 90,
            contentRange: 'bytes 10-99/200',
            // Buffer.fill(value, offset, end)
            expectedBuff: Buffer.allocUnsafe(90).fill(0, 0, 40).fill(1, 40),
        },
        { it: 'should get a range for an object using only an end ' +
            'offset in the request',
            range: 'bytes=-10',
            contentLength: 10,
            contentRange: 'bytes 190-199/200',
            expectedBuff: Buffer.alloc(10, 1),
        },
        { it: 'should get a range for an object using only a start offset ' +
            'in the request',
            range: 'bytes=190-',
            contentLength: 10,
            contentRange: 'bytes 190-199/200',
            expectedBuff: Buffer.alloc(10, 1),
        },
        { it: 'should get full object if range header is invalid',
            range: 'bytes=-',
            contentLength: 200,
            // Since range header is invalid full object should be returned
            // and there should be no Content-Range header
            contentRange: undefined,
            expectedBuff: Buffer.allocUnsafe(200).fill(0, 0, 50).fill(1, 50),
        },
    ];

    regularObjectRangeGetTests.forEach(test => {
        it(test.it, async () => {
            const params = {
                Bucket: bucket,
                Key: 'normalput',
                Range: test.range,
            };
            const data = await s3.send(new GetObjectCommand(params));
            assert.strictEqual(data.AcceptRanges, 'bytes');
            assert.strictEqual(data.ContentLength, test.contentLength);
            assert.strictEqual(data.ContentRange, test.contentRange);
            const chunks = [];
            for await (const chunk of data.Body) {
                chunks.push(chunk);
            }
            const body = Buffer.concat(chunks);
            assert.deepStrictEqual(body, test.expectedBuff);
        });
    });

    it('should delete an object put without MPU', async () => {
        // deleteObject test
        const params = {
            Bucket: bucket,
            Key: 'normalput',
        };
        const data = await s3.send(new DeleteObjectCommand(params));
        assert.ok(data);
    });

    // deletebucket test
    it('should delete a bucket', async () => {
        await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
    });
});
