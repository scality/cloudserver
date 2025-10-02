const assert = require('assert');
const {
    CreateBucketCommand,
    HeadObjectCommand,
    PutObjectCommand,
    DeleteObjectCommand,
    DeleteBucketCommand,
    CreateMultipartUploadCommand,
    UploadPartCommand,
    CompleteMultipartUploadCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const { maximumAllowedPartCount } = require('../../../../../constants');

const bucket = 'mpu-test-bucket';
const object = 'mpu-test-object';
const emptyObject = 'empty-object';
const nonMpuObject = 'simple-object';

/** 5MiB */
const bodySize = 1024 * 1024 * 5;
const bodyContent = 'a';
const howManyParts = 3;
const partNumbers = Array.from(Array(howManyParts).keys());
const invalidPartNumbers = [-1, 0, maximumAllowedPartCount + 1];

let ETags = [];

// Because HEAD has no body, the SDK (v2) returns a generic code such as:
// 400 BadRequest
// 403 Forbidden
// 404 NotFound
// ...
// It will fall back to HTTP statusCode
// Example: 416 InvalidRange will be 416 416
function checkError(err, statusCode, code) {
    if (err.$metadata && err.$metadata.httpStatusCode) {
        assert.strictEqual(err.$metadata.httpStatusCode, statusCode);
    } else if (err.Code) {
        assert.strictEqual(err.Code, code);
    }
}

function checkNoError(err) {
    assert.equal(err, null,
        `Expected success, got error ${JSON.stringify(err)}`);
}

function generateContent(partNumber) {
    return Buffer.alloc(bodySize + partNumber, bodyContent);
}

describe('Part size tests with object head', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;
        let uploadId;

        function headObject(fields, cb) {
            s3.send(new HeadObjectCommand({
                Bucket: bucket,
                Key: object,
                ...fields,
            })).then(data => {
                cb(null, data);
            }).catch(err => {
                cb(err);
            });
        }

        before(async () => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;

            // Create bucket
            await s3.send(new CreateBucketCommand({ Bucket: bucket }));

            // Create multipart upload
            const uploadResult = await s3.send(new CreateMultipartUploadCommand({
                Bucket: bucket,
                Key: object
            }));
            uploadId = uploadResult.UploadId;

            // Upload parts
            const uploadPromises = partNumbers.map(async partNumber => {
                const uploadPartParams = {
                    Bucket: bucket,
                    Key: object,
                    PartNumber: partNumber + 1,
                    UploadId: uploadId,
                    Body: generateContent(partNumber + 1),
                };
                const result = await s3.send(new UploadPartCommand(uploadPartParams));
                return result.ETag;
            });

            ETags = await Promise.all(uploadPromises);

            // Put empty object
            await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: emptyObject,
                Body: '',
            }));

            // Put non-MPU object
            await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: nonMpuObject,
                Body: generateContent(0),
            }));

            // Complete multipart upload
            const completeParams = {
                Bucket: bucket,
                Key: object,
                MultipartUpload: {
                    Parts: partNumbers.map(partNumber => ({
                        ETag: ETags[partNumber],
                        PartNumber: partNumber + 1,
                    })),
                },
                UploadId: uploadId,
            };
            await s3.send(new CompleteMultipartUploadCommand(completeParams));
        });

        after(async () => {
            await s3.send(new DeleteObjectCommand({
                Bucket: bucket,
                Key: object
            }));
            await s3.send(new DeleteObjectCommand({
                Bucket: bucket,
                Key: emptyObject
            }));
            await s3.send(new DeleteObjectCommand({
                Bucket: bucket,
                Key: nonMpuObject
            }));
            await s3.send(new DeleteBucketCommand({
                Bucket: bucket
            }));
        });

        it('should return the total size of the object ' +
            'when --part-number is not used', done => {
                const totalSize = partNumbers.reduce((total, current) =>
                    total + (bodySize + current + 1), 0);
                headObject({}, (err, data) => {
                    checkNoError(err);
                    assert.equal(totalSize, data.ContentLength);
                    done();
                });
            });

        partNumbers.forEach(part => {
            it(`should return the size of part ${part + 1} ` +
                `when --part-number is set to ${part + 1}`, done => {
                const partNumber = Number.parseInt(part, 10) + 1;
                const partSize = bodySize + partNumber;
                headObject({ PartNumber: partNumber }, (err, data) => {
                    checkNoError(err);
                    assert.equal(partSize, data.ContentLength);
                    done();
                });
            });
        });

        invalidPartNumbers.forEach(part => {
            it(`should return an error when --part-number is set to ${part}`,
            done => {
                headObject({ PartNumber: part }, (err, data) => {
                    checkError(err, 400, 'BadRequest');
                    assert.strictEqual(data, undefined);
                    done();
                });
            });
        });

        it('should return an error when incorrect --part-number is used',
            done => {
                headObject({ PartNumber: partNumbers.length + 1 },
                (err, data) => {
                    checkError(err, 416, 416);
                    assert.strictEqual(data, undefined);
                    done();
                });
            });

        it('should return content-length 0 when requesting part 1 of empty object', done => {
            headObject({ Key: emptyObject, PartNumber: 1 }, (err, data) => {
                checkNoError(err);
                assert.strictEqual(data.ContentLength, 0);
                done();
            });
        });

        it('should return an error when requesting part 2 of empty object', done => {
            headObject({ Key: emptyObject, PartNumber: 2 }, (err, data) => {
                checkError(err, 416, 416);
                assert.strictEqual(data, undefined);
                done();
            });
        });

        it('should return content-length requesting part 1 of non-MPU object', done => {
            headObject({ Key: nonMpuObject, PartNumber: 1 }, (err, data) => {
                checkNoError(err);
                assert.strictEqual(data.ContentLength, bodySize);
                done();
            });
        });

        it('should return an error when requesting part 2 of non-MPU object', done => {
            headObject({ Key: nonMpuObject, PartNumber: 2 }, (err, data) => {
                checkError(err, 416, 416);
                assert.strictEqual(data, undefined);
                done();
            });
        });
    });
});
