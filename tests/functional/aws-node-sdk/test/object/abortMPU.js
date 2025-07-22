const assert = require('assert');
const { v4: uuidv4 } = require('uuid');
const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const async = require('async');

const date = Date.now();
const bucket = `abortmpu${date}`;
const key = 'key';
const bodyFirstPart = Buffer.allocUnsafe(10).fill(0);

function checkError(err, code, message) {
    assert.notEqual(err, null, 'Expected failure but got success');
    assert.strictEqual(err.code, code);
    assert.strictEqual(err.message, message);
}

describe('Abort MPU', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;
        let uploadId;

        beforeEach(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return s3
                .createBucket({ Bucket: bucket })
                .promise()
                .then(() =>
                    s3
                        .createMultipartUpload({
                            Bucket: bucket,
                            Key: key,
                        })
                        .promise()
                )
                .then(res => {
                    uploadId = res.UploadId;
                    return s3
                        .uploadPart({
                            Bucket: bucket,
                            Key: key,
                            PartNumber: 1,
                            UploadId: uploadId,
                            Body: bodyFirstPart,
                        })
                        .promise();
                })
                .catch(err => {
                    process.stdout.write(`Error in beforeEach: ${err}\n`);
                    throw err;
                });
        });

        afterEach(() =>
            s3
                .abortMultipartUpload({
                    Bucket: bucket,
                    Key: key,
                    UploadId: uploadId,
                })
                .promise()
                .then(() => bucketUtil.empty(bucket))
                .then(() => bucketUtil.deleteOne(bucket))
        );

        // aws-sdk now (v2.363.0) returns 'UriParameterError' error
        // this test was not replaced in any other suite
        it.skip('should return InvalidRequest error if aborting without key', done => {
            s3.abortMultipartUpload(
                {
                    Bucket: bucket,
                    Key: '',
                    UploadId: uploadId,
                },
                err => {
                    checkError(err, 'InvalidRequest', 'A key must be specified');
                    done();
                }
            );
        });
    });
});

describe('Abort MPU with existing object', function AbortMPUExistingObject() {
    this.timeout(60000);

    withV4(sigCfg => {
        let bucketUtil;
        let s3;
        const bucketName = `abortmpu-test-bucket-${Date.now()}`;
        const objectKey = 'my-object';

        beforeEach(done => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            s3.createBucket({ Bucket: bucketName }, err => {
                assert.ifError(err, `Error creating bucket: ${err}`);
                done();
            });
        });

        afterEach(async () => {
            const data = await s3.listMultipartUploads({ Bucket: bucketName }).promise();
            const uploads = data.Uploads;
            await Promise.all(
                uploads.map(async upload => {
                    try {
                        await s3
                            .abortMultipartUpload({
                                Bucket: bucketName,
                                Key: upload.Key,
                                UploadId: upload.UploadId,
                            })
                            .promise();
                    } catch (err) {
                        if (err.code !== 'NoSuchUpload') {
                            throw err;
                        }
                        // If NoSuchUpload, swallow error
                    }
                })
            );
            await bucketUtil.empty(bucketName);
            await bucketUtil.deleteOne(bucketName);
        });

        it('should not delete existing object data when aborting another MPU for same key', done => {
            const part1 = Buffer.from('I am part 1 of MPU 1');
            const part2 = Buffer.from('I am part 1 of MPU 2');
            let uploadId1;
            let uploadId2;
            let etag1;
            async.waterfall(
                [
                    next => {
                        s3.createMultipartUpload({ Bucket: bucketName, Key: objectKey }, (err, data) => {
                            assert.ifError(err, `error creating MPU 1: ${err}`);
                            uploadId1 = data.UploadId;
                            s3.uploadPart(
                                {
                                    Bucket: bucketName,
                                    Key: objectKey,
                                    PartNumber: 1,
                                    UploadId: uploadId1,
                                    Body: part1,
                                },
                                (err, data) => {
                                    assert.ifError(err, `error uploading part for MPU 1: ${err}`);
                                    etag1 = data.ETag;
                                    s3.completeMultipartUpload(
                                        {
                                            Bucket: bucketName,
                                            Key: objectKey,
                                            UploadId: uploadId1,
                                            MultipartUpload: { Parts: [{ ETag: etag1, PartNumber: 1 }] },
                                        },
                                        err => {
                                            assert.ifError(err, `error completing MPU 1: ${err}`);
                                            next();
                                        }
                                    );
                                }
                            );
                        });
                    },
                    next => {
                        s3.getObject({ Bucket: bucketName, Key: objectKey }, (err, data) => {
                            assert.ifError(err, `error getting object after MPU 1: ${err}`);
                            assert.strictEqual(data.Body.toString(), part1.toString());
                            next();
                        });
                    },
                    next => {
                        s3.createMultipartUpload({ Bucket: bucketName, Key: objectKey }, (err, data) => {
                            assert.ifError(err, `error creating MPU 2: ${err}`);
                            uploadId2 = data.UploadId;
                            s3.uploadPart(
                                {
                                    Bucket: bucketName,
                                    Key: objectKey,
                                    PartNumber: 1,
                                    UploadId: uploadId2,
                                    Body: part2,
                                },
                                err => {
                                    assert.ifError(err, `error uploading part for MPU 2: ${err}`);
                                    next();
                                }
                            );
                        });
                    },
                    next => {
                        s3.abortMultipartUpload({ Bucket: bucketName, Key: objectKey, UploadId: uploadId2 }, err => {
                            assert.ifError(err, `error aborting MPU 2: ${err}`);
                            next();
                        });
                    },
                    next => {
                        s3.getObject({ Bucket: bucketName, Key: objectKey }, (err, data) => {
                            assert.ifError(err, `error getting object after aborting MPU 2: ${err}`);
                            assert.strictEqual(data.Body.toString(), part1.toString());
                            next();
                        });
                    },
                ],
                done
            );
        });

        it('should not delete existing object data when aborting an old MPU for same key', done => {
            const part1 = Buffer.from('I am part 1 of MPU 1');
            const part2 = Buffer.from('I am part 1 of MPU 2');
            let uploadId1;
            let uploadId2;
            let etag2;
            async.waterfall(
                [
                    next => {
                        s3.createMultipartUpload(
                            {
                                Bucket: bucketName,
                                Key: objectKey,
                            },
                            (err, data) => {
                                assert.ifError(err, `error creating MPU 1: ${err}`);
                                uploadId1 = data.UploadId;
                                s3.uploadPart(
                                    {
                                        Bucket: bucketName,
                                        Key: objectKey,
                                        PartNumber: 1,
                                        UploadId: uploadId1,
                                        Body: part1,
                                    },
                                    err => {
                                        assert.ifError(err, `error uploading part for MPU 1: ${err}`);
                                        next();
                                    }
                                );
                            }
                        );
                    },
                    next => {
                        s3.createMultipartUpload(
                            {
                                Bucket: bucketName,
                                Key: objectKey,
                            },
                            (err, data) => {
                                assert.ifError(err, `error creating MPU 2: ${err}`);
                                uploadId2 = data.UploadId;
                                s3.uploadPart(
                                    {
                                        Bucket: bucketName,
                                        Key: objectKey,
                                        PartNumber: 1,
                                        UploadId: uploadId2,
                                        Body: part2,
                                    },
                                    (err, data) => {
                                        assert.ifError(err, `error uploading part for MPU 2: ${err}`);
                                        etag2 = data.ETag;
                                        s3.completeMultipartUpload(
                                            {
                                                Bucket: bucketName,
                                                Key: objectKey,
                                                UploadId: uploadId2,
                                                MultipartUpload: { Parts: [{ ETag: etag2, PartNumber: 1 }] },
                                            },
                                            err => {
                                                assert.ifError(err, `error completing MPU 2: ${err}`);
                                                next();
                                            }
                                        );
                                    }
                                );
                            }
                        );
                    },
                    next => {
                        s3.getObject(
                            {
                                Bucket: bucketName,
                                Key: objectKey,
                            },
                            (err, data) => {
                                assert.ifError(err, `error getting object after MPU 2: ${err}`);
                                assert.strictEqual(data.Body.toString(), part2.toString());
                                next();
                            }
                        );
                    },
                    next => {
                        s3.abortMultipartUpload(
                            {
                                Bucket: bucketName,
                                Key: objectKey,
                                UploadId: uploadId1,
                            },
                            err => {
                                assert.ifError(err, `error aborting MPU 1: ${err}`);
                                next();
                            }
                        );
                    },
                    next => {
                        s3.getObject({ Bucket: bucketName, Key: objectKey }, (err, data) => {
                            assert.ifError(err, `error getting object after aborting MPU 1: ${err}`);
                            assert.strictEqual(data.Body.toString(), part2.toString());
                            next();
                        });
                    },
                ],
                done
            );
        });
    });
});

describe('Abort MPU - No Such Upload', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        beforeEach(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return s3.createBucket({ Bucket: bucket }).promise();
        });

        afterEach(() => bucketUtil.deleteOne(bucket));

        it('should return NoSuchUpload error when aborting non-existent mpu', done => {
            s3.abortMultipartUpload(
                {
                    Bucket: bucket,
                    Key: key,
                    UploadId: uuidv4().replace(/-/g, ''),
                },
                err => {
                    assert.notEqual(err, null, 'Expected failure but got success');
                    assert.strictEqual(err.code, 'NoSuchUpload');
                    done();
                }
            );
        });
    });
});
