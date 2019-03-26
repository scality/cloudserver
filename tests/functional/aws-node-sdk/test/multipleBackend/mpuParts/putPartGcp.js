const assert = require('assert');
const async = require('async');
const arsenal = require('arsenal');

const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const { describeSkipIfNotMultipleOrCeph, gcpClient, gcpBucket, gcpBucketMPU,
    gcpLocation, gcpLocationMismatch, uniqName, genUniqID }
    = require('../utils');
const { createMpuKey } = arsenal.storage.data.external.GcpUtils;

const keyObject = 'putgcp';
const bucket = `putpartgcp${genUniqID()}`;
const body = Buffer.from('I am a body', 'utf8');
const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
const emptyMD5 = 'd41d8cd98f00b204e9800998ecf8427e';

let bucketUtil;
let s3;

function checkMPUResult(bucket, key, uploadId, objCount, expected, cb) {
    const params = {
        Bucket: bucket,
        Key: key,
        UploadId: uploadId,
    };
    gcpClient.listParts(params, (err, res) => {
        assert.ifError(err,
            `Expected success, but got err ${err}`);
        expect(res && res.Contents &&
            res.Contents.length === objCount).toBeTruthy();
        res.Contents.forEach(part => {
            expect(part.ETag).toBe(`"${expected}"`);
        });
        cb();
    });
}

describeSkipIfNotMultipleOrCeph('MultipleBacked put part to GCP', function
describeFn() {
    this.timeout(180000);
    withV4(sigCfg => {
        beforeEach(() => {
            this.currentTest.key = uniqName(keyObject);
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
        });

        describe('with bucket location header', () => {
            let testContext;

            beforeEach(() => {
                testContext = {};
            });

            beforeEach(done => {
                async.waterfall([
                    next => s3.createBucket({ Bucket: bucket,
                    }, err => next(err)),
                    next => s3.createMultipartUpload({
                        Bucket: bucket,
                        Key: testContext.currentTest.key,
                        Metadata: { 'scal-location-constraint': gcpLocation },
                    }, (err, res) => {
                        if (err) {
                            return next(err);
                        }
                        testContext.currentTest.uploadId = res.UploadId;
                        return next();
                    }),
                ], done);
            });

            afterEach(done => {
                async.waterfall([
                    next => s3.abortMultipartUpload({
                        Bucket: bucket,
                        Key: testContext.currentTest.key,
                        UploadId: testContext.currentTest.uploadId,
                    }, err => next(err)),
                    next => s3.deleteBucket({ Bucket: bucket },
                      err => next(err)),
                ], err => {
                    expect(err).toEqual(null);
                    done();
                });
            });

            test('should put 0-byte part to GCP', done => {
                const params = {
                    Bucket: bucket,
                    Key: testContext.test.key,
                    UploadId: testContext.test.uploadId,
                    PartNumber: 1,
                };
                async.waterfall([
                    next => s3.uploadPart(params, (err, res) => {
                        assert.ifError(err,
                            `Expected success, but got err ${err}`);
                        expect(res.ETag).toBe(`"${emptyMD5}"`);
                        next();
                    }),
                    next => {
                        const mpuKey =
                            createMpuKey(testContext.test.key, testContext.test.uploadId, 1);
                        const getParams = {
                            Bucket: gcpBucketMPU,
                            Key: mpuKey,
                        };
                        gcpClient.getObject(getParams, (err, res) => {
                            assert.ifError(err,
                                `Expected success, but got err ${err}`);
                            expect(res.ETag).toBe(`"${emptyMD5}"`);
                            next();
                        });
                    },
                ], done);
            });

            test('should put 2 parts to GCP', done => {
                async.waterfall([
                    next => {
                        async.times(2, (n, cb) => {
                            const params = {
                                Bucket: bucket,
                                Key: testContext.test.key,
                                UploadId: testContext.test.uploadId,
                                Body: body,
                                PartNumber: n + 1,
                            };
                            s3.uploadPart(params, (err, res) => {
                                assert.ifError(err,
                                    `Expected success, but got err ${err}`);
                                expect(res.ETag).toBe(`"${correctMD5}"`);
                                cb();
                            });
                        }, () => next());
                    },
                    next => checkMPUResult(
                        gcpBucketMPU, testContext.test.key, testContext.test.uploadId,
                        2, correctMD5, next),
                ], done);
            });

            test('should put the same part twice', done => {
                async.waterfall([
                    next => {
                        const partBody = ['', body];
                        const partMD5 = [emptyMD5, correctMD5];
                        async.timesSeries(2, (n, cb) => {
                            const params = {
                                Bucket: bucket,
                                Key: testContext.test.key,
                                UploadId: testContext.test.uploadId,
                                Body: partBody[n],
                                PartNumber: 1,
                            };
                            s3.uploadPart(params, (err, res) => {
                                assert.ifError(err,
                                    `Expected success, but got err ${err}`);
                                expect(res.ETag).toBe(`"${partMD5[n]}"`);
                                cb();
                            });
                        }, () => next());
                    },
                    next => checkMPUResult(
                        gcpBucketMPU, testContext.test.key, testContext.test.uploadId,
                        1, correctMD5, next),
                ], done);
            });
        });

        describe('with same key as preexisting part', () => {
            let testContext;

            beforeEach(() => {
                testContext = {};
            });

            beforeEach(done => {
                async.waterfall([
                    next => s3.createBucket({ Bucket: bucket },
                        err => next(err)),
                    next => {
                        s3.putObject({
                            Bucket: bucket,
                            Key: testContext.currentTest.key,
                            Metadata: {
                                'scal-location-constraint': gcpLocation },
                            Body: body,
                        }, err => {
                            expect(err).toEqual(null);
                            return next();
                        });
                    },
                    next => s3.createMultipartUpload({
                        Bucket: bucket,
                        Key: testContext.currentTest.key,
                        Metadata: { 'scal-location-constraint': gcpLocation },
                    }, (err, res) => {
                        if (err) {
                            return next(err);
                        }
                        testContext.currentTest.uploadId = res.UploadId;
                        return next();
                    }),
                ], done);
            });

            afterEach(done => {
                async.waterfall([
                    next => {
                        process.stdout.write('Aborting multipart upload\n');
                        s3.abortMultipartUpload({
                            Bucket: bucket,
                            Key: testContext.currentTest.key,
                            UploadId: testContext.currentTest.uploadId },
                        err => next(err));
                    },
                    next => {
                        process.stdout.write('Deleting object\n');
                        s3.deleteObject({
                            Bucket: bucket,
                            Key: testContext.currentTest.key },
                        err => next(err));
                    },
                    next => {
                        process.stdout.write('Deleting bucket\n');
                        s3.deleteBucket({
                            Bucket: bucket },
                        err => next(err));
                    },
                ], err => {
                    expect(err).toEqual(null);
                    done();
                });
            });

            test(
                'should put a part without overwriting existing object',
                done => {
                    const body = Buffer.alloc(20);
                    s3.uploadPart({
                        Bucket: bucket,
                        Key: testContext.test.key,
                        UploadId: testContext.test.uploadId,
                        PartNumber: 1,
                        Body: body,
                    }, err => {
                        expect(err).toBe(null);
                        gcpClient.getObject({
                            Bucket: gcpBucket,
                            Key: testContext.test.key,
                        }, (err, res) => {
                            assert.ifError(err,
                                `Expected success, but got err ${err}`);
                            expect(res.ETag).toBe(`"${correctMD5}"`);
                            done();
                        });
                    });
                }
            );
        });
    });
});

describeSkipIfNotMultipleOrCeph('MultipleBackend put part to GCP location ' +
'with bucketMatch sets to false', function
describeF() {
    this.timeout(80000);
    withV4(sigCfg => {
        beforeEach(() => {
            this.currentTest.key = uniqName(keyObject);
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
        });
        describe('with bucket location header', () => {
            let testContext;

            beforeEach(() => {
                testContext = {};
            });

            beforeEach(done => {
                async.waterfall([
                    next => s3.createBucket({ Bucket: bucket,
                    }, err => next(err)),
                    next => s3.createMultipartUpload({
                        Bucket: bucket,
                        Key: testContext.currentTest.key,
                        Metadata: { 'scal-location-constraint':
                        gcpLocationMismatch },
                    }, (err, res) => {
                        if (err) {
                            return next(err);
                        }
                        testContext.currentTest.uploadId = res.UploadId;
                        return next();
                    }),
                ], done);
            });

            afterEach(done => {
                async.waterfall([
                    next => s3.abortMultipartUpload({
                        Bucket: bucket,
                        Key: testContext.currentTest.key,
                        UploadId: testContext.currentTest.uploadId,
                    }, err => next(err)),
                    next => s3.deleteBucket({ Bucket: bucket },
                      err => next(err)),
                ], err => {
                    expect(err).toEqual(null);
                    done();
                });
            });

            test('should put part to GCP location with bucketMatch' +
            ' sets to false', done => {
                const body20 = Buffer.alloc(20);
                const params = {
                    Bucket: bucket,
                    Key: testContext.test.key,
                    UploadId: testContext.test.uploadId,
                    PartNumber: 1,
                    Body: body20,
                };
                const eTagExpected =
                    '"441018525208457705bf09a8ee3c1093"';
                async.waterfall([
                    next => s3.uploadPart(params, (err, res) => {
                        expect(res.ETag).toBe(eTagExpected);
                        next(err);
                    }),
                    next => {
                        const key =
                            createMpuKey(testContext.test.key, testContext.test.uploadId, 1);
                        const mpuKey = `${bucket}/${key}`;
                        const getParams = {
                            Bucket: gcpBucketMPU,
                            Key: mpuKey,
                        };
                        gcpClient.getObject(getParams, (err, res) => {
                            assert.ifError(err,
                                `Expected success, but got err ${err}`);
                            expect(res.ETag).toBe(eTagExpected);
                            next();
                        });
                    },
                ], done);
            });
        });
    });
});
