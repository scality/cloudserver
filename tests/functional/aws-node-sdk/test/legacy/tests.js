const assert = require('assert');
const crypto = require('crypto');
const { S3 } = require('aws-sdk');

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

describe('aws-node-sdk test suite as registered user', () => {
    this.timeout(60000);
    let s3;

    // setup test
    beforeAll(() => {
        const config = getConfig('default', { signatureVersion: 'v4' });

        s3 = new S3(config);
    });

    // bucketListing test
    test('should do bucket listing', done => {
        s3.listBuckets((err, data) => {
            if (err) {
                return done(new Error(`error listing buckets: ${err}`));
            }

            expect(data.Buckets).toBeTruthy();
            expect(data.Owner).toBeTruthy();
            expect(data.Owner.ID).toBeTruthy();
            expect(data.Owner.DisplayName).toBeTruthy();
            const owner = Object.keys(data.Owner);
            expect(owner.length).toBe(2);
            return done();
        });
    });

    // createbucket test
    test('should create a bucket', done => {
        s3.createBucket({ Bucket: bucket }, err => {
            if (err) {
                return done(new Error(`error creating bucket: ${err}`));
            }
            return done();
        });
    });

    // createMPU test
    test('should create a multipart upload', done => {
        s3.createMultipartUpload({ Bucket: bucket, Key: objectKey },
            (err, data) => {
                if (err) {
                    return done(new Error(
                        `error initiating multipart upload: ${err}`));
                }
                expect(data.Bucket).toBe(bucket);
                expect(data.Key).toBe(objectKey);
                expect(data.UploadId).toBeTruthy();
                multipartUploadData.firstUploadId = data.UploadId;
                return done();
            });
    });

    test(
        'should upload a part of a multipart upload to be aborted',
        // uploadpart test
        done => {
            const params = {
                Bucket: bucket,
                Key: objectKey,
                PartNumber: 1,
                UploadId: multipartUploadData.firstUploadId,
                Body: firstBufferBody,
            };
            s3.uploadPart(params, (err, data) => {
                if (err) {
                    return done(new Error(`error uploading a part: ${err}`));
                }
                expect(data.ETag).toBe(`"${calculatedFirstPartHash}"`);
                return done();
            });
        }
    );

    // abortMPU test
    test('should abort a multipart upload', done => {
        const params = {
            Bucket: bucket,
            Key: objectKey,
            UploadId: multipartUploadData.firstUploadId,
        };
        s3.abortMultipartUpload(params, (err, data) => {
            if (err) {
                return done(new Error(
                    `error aborting multipart upload: ${err}`));
            }
            expect(data).toBeTruthy();
            return done();
        });
    });

    // createMPU test
    test('should upload a part of a multipart upload', done => {
        s3.createMultipartUpload({ Bucket: bucket, Key: 'toComplete' },
            (err, data) => {
                if (err) {
                    return done(new Error(
                        `error initiating multipart upload: ${err}`));
                }
                const uploadId = data.UploadId;
                multipartUploadData.secondUploadId = data.UploadId;
                const params = {
                    Bucket: bucket,
                    Key: 'toComplete',
                    PartNumber: 1,
                    UploadId: uploadId,
                    Body: firstBufferBody,
                };
                s3.uploadPart(params, (err, data) => {
                    if (err) {
                        return done(
                            new Error(`error uploading a part: ${err}`));
                    }
                    expect(data.ETag).toBe(`"${calculatedFirstPartHash}"`);
                    return done();
                });
                return undefined;
            });
    });

    test('should upload a second part of a multipart upload', // createMPU test
    done => {
        const params = {
            Bucket: bucket,
            Key: 'toComplete',
            PartNumber: 2,
            UploadId: multipartUploadData.secondUploadId,
            Body: secondBufferBody,
        };
        s3.uploadPart(params, (err, data) => {
            if (err) {
                return done(new Error(`error uploading a part: ${err}`));
            }
            expect(data.ETag).toBe(`"${calculatedSecondPartHash}"`);
            return done();
        });
    });

    // listparts test
    test('should list the parts of a multipart upload', done => {
        const params = {
            Bucket: bucket,
            Key: 'toComplete',
            UploadId: multipartUploadData.secondUploadId,
        };
        s3.listParts(params, (err, data) => {
            if (err) {
                return done(new Error(`error listing parts: ${err}`));
            }
            expect(data.Bucket).toBe(bucket);
            expect(data.Key).toBe('toComplete');
            expect(data.UploadId).toBe(multipartUploadData
                .secondUploadId);
            expect(data.IsTruncated).toBe(false);
            expect(data.Parts[0].PartNumber).toBe(1);
            expect(data.Parts[0].ETag).toBe(`"${calculatedFirstPartHash}"`);
            expect(data.Parts[0].Size).toBe(5242880);
            expect(data.Parts[1].PartNumber).toBe(2);
            expect(data.Parts[1].ETag).toBe(`"${calculatedSecondPartHash}"`);
            expect(data.Parts[1].Size).toBe(5242880);
            // Must disable for now when running with Vault
            // since will need to pull actual ARN and canonicalId
            // assert.strictEqual(data.Initiator.ID, accessKey1ARN);
            // Note that for in memory implementation, "accessKey1"
            // is both the access key and the canonicalId so this
            // call works.  For real implementation with vault,
            // will need the canonicalId.
            // assert.strictEqual(data.Owner.ID, config.accessKeyId);
            expect(data.StorageClass).toBe('STANDARD');
            return {};
        });
        return done();
    });

    test('should return an error if do not provide correct ' +
        // completempu test
        'xml when completing a multipart upload', done => {
        const params = {
            Bucket: bucket,
            Key: 'toComplete',
            UploadId: multipartUploadData.secondUploadId,
        };
        s3.completeMultipartUpload(params, err => {
            expect(err.code).toBe('MalformedXML');
            return done();
        });
    });

    // completempu test
    test('should complete a multipart upload', done => {
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
        s3.completeMultipartUpload(params, (err, data) => {
            if (err) {
                return done(new Error(`error completing mpu: ${err}`));
            }
            expect(data.Bucket).toBe(bucket);
            expect(data.Key).toBe('toComplete');
            expect(data.ETag).toBe(combinedETag);
            return done();
        });
    });

    test('should get an object put by multipart upload', done => {
        const params = {
            Bucket: bucket,
            Key: 'toComplete',
        };
        s3.getObject(params, (err, data) => {
            if (err) {
                return done(new Error(
                    `error getting object put by mpu: ${err}`));
            }
            expect(data.ETag).toBe(combinedETag);
            const uploadedObj = Buffer.concat([firstBufferBody,
                secondBufferBody]);
            assert.deepStrictEqual(data.Body, uploadedObj);
            return done();
        });
    });

    const mpuRangeGetTests = [
        { it: 'should get a range from the first part of an object ' +
            'put by multipart upload',
            range: 'bytes=0-9',
            contentLength: '10',
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
            contentLength: '10',
            contentRange: 'bytes 5242880-5242889/10485760',
            // A range from the second part should just contain 1
            expectedBuff: Buffer.alloc(10, 1),
        },
        { it: 'should get a range that spans both parts of an object put ' +
            'by multipart upload',
            range: 'bytes=5242875-5242884',
            contentLength: '10',
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
            contentLength: '10',
            contentRange: 'bytes 10485750-10485759/10485760',
            // Range from the second part should just contain 1
            expectedBuff: Buffer.alloc(10, 1),
        },
        {
            it: 'should get entire object if range is invalid',
            range: 'bytes=-10485761',
            contentLength: '10485760',
            contentRange: 'bytes 0-10485759/10485760',
            expectedBuff: Buffer.concat([firstBufferBody, secondBufferBody]),
        },
    ];

    mpuRangeGetTests.forEach(test => {
        test(test.it, done => {
            const params = {
                Bucket: bucket,
                Key: 'toComplete',
                Range: test.range,
            };
            s3.getObject(params, (err, data) => {
                if (err) {
                    return done(new Error(
                        `error getting object range put by mpu: ${err}`));
                }
                expect(data.ContentLength).toBe(test.contentLength);
                expect(data.AcceptRanges).toBe('bytes');
                expect(data.ContentRange).toBe(test.contentRange);
                expect(data.ETag).toBe(combinedETag);
                assert.deepStrictEqual(data.Body, test.expectedBuff);
                return done();
            });
        });
    });

    test('should delete object created by multipart upload', // deleteObject test
    done => {
        const params = {
            Bucket: bucket,
            Key: 'toComplete',
        };
        s3.deleteObject(params, (err, data) => {
            if (err) {
                return done(new Error(`error deleting object: ${err}`));
            }
            expect(data).toBeTruthy();
            return done();
        });
    });

    test('should put an object regularly (non-MPU)', done => {
        const params = {
            Bucket: bucket,
            Key: 'normalput',
            Body: Buffer.allocUnsafe(200).fill(0, 0, 50).fill(1, 50),
        };
        s3.putObject(params, (err, data) => {
            if (err) {
                return done(new Error(
                    `error putting object regularly: ${err}`));
            }
            expect(data).toBeTruthy();
            return done();
        });
    });

    test('should return InvalidRange if the range of the resource does ' +
    'not cover the byte range', done => {
        const params = {
            Bucket: bucket,
            Key: 'normalput',
            Range: 'bytes=200-200',
        };
        s3.getObject(params, err => {
            expect(err).not.toEqual(null);
            expect(err.code).toBe('InvalidRange');
            return done();
        });
    });

    describe('Get range on empty object', () => {
        const params = {
            Bucket: bucketEmptyObj,
            Key: 'emptyobj',
        };
        beforeEach(done => {
            s3.createBucket({ Bucket: bucketEmptyObj }, err => {
                if (err) {
                    return done(new Error(`error creating bucket: ${err}`));
                }
                return s3.putObject(params, err => {
                    if (err) {
                        return done(new Error(
                            `error putting object regularly: ${err}`));
                    }
                    return done();
                });
            });
        });
        afterEach(done => {
            s3.deleteObject(params, err => {
                if (err) {
                    return done(new Error(
                        `error deletting object regularly: ${err}`));
                }
                return s3.deleteBucket({ Bucket: bucketEmptyObj }, err => {
                    if (err) {
                        return done(new Error(`error deleting bucket: ${err}`));
                    }
                    return done();
                });
            });
        });
        testsRangeOnEmptyFile.forEach(test => {
            const validText = test.valid ? 'InvalidRange error' : 'empty file';
            test(`should return ${validText} if get range ${test.range} on ` +
            'empty object', done => {
                const params = {
                    Bucket: bucketEmptyObj,
                    Key: 'emptyobj',
                    Range: test.range,
                };
                s3.getObject(params, (err, data) => {
                    if (test.valid) {
                        expect(err).not.toEqual(null);
                        expect(err.code).toBe('InvalidRange');
                    } else {
                        expect(err).toEqual(null);
                        expect(data.Body.toString()).toBe('');
                    }
                    return done();
                });
            });
        });
    });

    const regularObjectRangeGetTests = [
        { it: 'should get a range for an object put without MPU',
            range: 'bytes=10-99',
            contentLength: '90',
            contentRange: 'bytes 10-99/200',
            // Buffer.fill(value, offset, end)
            expectedBuff: Buffer.allocUnsafe(90).fill(0, 0, 40).fill(1, 40),
        },
        { it: 'should get a range for an object using only an end ' +
            'offset in the request',
            range: 'bytes=-10',
            contentLength: '10',
            contentRange: 'bytes 190-199/200',
            expectedBuff: Buffer.alloc(10, 1),
        },
        { it: 'should get a range for an object using only a start offset ' +
            'in the request',
            range: 'bytes=190-',
            contentLength: '10',
            contentRange: 'bytes 190-199/200',
            expectedBuff: Buffer.alloc(10, 1),
        },
        { it: 'should get full object if range header is invalid',
            range: 'bytes=-',
            contentLength: '200',
            // Since range header is invalid full object should be returned
            // and there should be no Content-Range header
            contentRange: undefined,
            expectedBuff: Buffer.allocUnsafe(200).fill(0, 0, 50).fill(1, 50),
        },
    ];

    regularObjectRangeGetTests.forEach(test => {
        test(test.it, done => {
            const params = {
                Bucket: bucket,
                Key: 'normalput',
                Range: test.range,
            };
            s3.getObject(params, (err, data) => {
                if (err) {
                    return done(new Error(
                        `error getting object range: ${err}`));
                }
                expect(data.AcceptRanges).toBe('bytes');
                expect(data.ContentLength).toBe(test.contentLength);
                expect(data.ContentRange).toBe(test.contentRange);
                assert.deepStrictEqual(data.Body, test.expectedBuff);
                return done();
            });
        });
    });

    test('should delete an object put without MPU', // deleteObject test
    done => {
        const params = {
            Bucket: bucket,
            Key: 'normalput',
        };
        s3.deleteObject(params, (err, data) => {
            if (err) {
                return done(new Error(`error deleting object: ${err}`));
            }
            expect(data).toBeTruthy();
            return done();
        });
    });

    // deletebucket test
    test('should delete a bucket', done => {
        s3.deleteBucket({ Bucket: bucket }, err => {
            if (err) {
                return done(new Error(`error deleting bucket: ${err}`));
            }
            return done();
        });
    });
});
