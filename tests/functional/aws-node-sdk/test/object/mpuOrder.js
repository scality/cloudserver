const assert = require('assert');
const {
    CreateBucketCommand,
    CreateMultipartUploadCommand,
    UploadPartCommand,
    CompleteMultipartUploadCommand,
    AbortMultipartUploadCommand,
    DeleteObjectCommand,
    DeleteBucketCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const bucket = 'bucketlistparts';
const object = 'toto';

function checkError(err, statusCode, code) {
    assert.strictEqual(err.$metadata.httpStatusCode, statusCode);
    assert.strictEqual(err.Code, code);
}

const body = Buffer.alloc(1024 * 1024 * 5, 'a');

const testsOrder = [
    { values: [3, 8, 1000], err: false },
    { values: [8, 3, 1000], err: true },
    { values: [8, 1000, 3], err: true },
    { values: [1000, 3, 8], err: true },
    { values: [3, 1000, 8], err: true },
    { values: [1000, 8, 3], err: true },
    { values: [3, 3, 1000], err: true },
];

describe('More MPU tests', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        beforeEach(async function beforeEachF() {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            await s3.send(new CreateBucketCommand({ Bucket: bucket }));
            const mpuRes = await s3.send(
                new CreateMultipartUploadCommand({
                    Bucket: bucket,
                    Key: object,
                }),
            );
            this.currentTest.UploadId = mpuRes.UploadId;
            const part1000Res = await s3.send(
                new UploadPartCommand({
                    Bucket: bucket,
                    Key: object,
                    PartNumber: 1000,
                    Body: body,
                    UploadId: this.currentTest.UploadId,
                }),
            );
            this.currentTest.Etag = part1000Res.ETag;
            await s3.send(
                new UploadPartCommand({
                    Bucket: bucket,
                    Key: object,
                    PartNumber: 3,
                    Body: body,
                    UploadId: this.currentTest.UploadId,
                }),
            );
            await s3.send(
                new UploadPartCommand({
                    Bucket: bucket,
                    Key: object,
                    PartNumber: 8,
                    Body: body,
                    UploadId: this.currentTest.UploadId,
                }),
            );
        });

        afterEach(async () => {
            await s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: object }));
            await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
        });

        testsOrder.forEach(testOrder => {
            it(
                'should complete MPU by concatenating the parts in ' + `the following order: ${testOrder.values}`,
                async function itF() {
                    try {
                        await s3.send(
                            new CompleteMultipartUploadCommand({
                                Bucket: bucket,
                                Key: object,
                                MultipartUpload: {
                                    Parts: [
                                        {
                                            ETag: this.test.Etag,
                                            PartNumber: testOrder.values[0],
                                        },
                                        {
                                            ETag: this.test.Etag,
                                            PartNumber: testOrder.values[1],
                                        },
                                        {
                                            ETag: this.test.Etag,
                                            PartNumber: testOrder.values[2],
                                        },
                                    ],
                                },
                                UploadId: this.test.UploadId,
                            }),
                        );

                        if (testOrder.err) {
                            throw new Error('Expected InvalidPartOrder error but operation succeeded');
                        }
                    } catch (err) {
                        if (testOrder.err) {
                            checkError(err, 400, 'InvalidPartOrder');
                            await s3.send(
                                new AbortMultipartUploadCommand({
                                    Bucket: bucket,
                                    Key: object,
                                    UploadId: this.test.UploadId,
                                }),
                            );
                        } else {
                            throw err;
                        }
                    }
                },
            );
        });
    });
});
