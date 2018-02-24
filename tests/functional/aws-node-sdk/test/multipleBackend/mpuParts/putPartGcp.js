const assert = require('assert');
const async = require('async');

const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const { describeSkipIfNotMultiple, gcpClient, gcpBucket, gcpBucketMPU,
    gcpLocation, gcpLocationMismatch, uniqName } = require('../utils');
const { createMpuKey } =
    require('../../../../../../lib/data/external/GCP').GcpUtils;

const keyObject = 'putgcp';
const bucket = 'buckettestmultiplebackendputpart-gcp';
const body = Buffer.from('I am a body', 'utf8');
const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
const emptyMD5 = 'd41d8cd98f00b204e9800998ecf8427e';
const skipIfNotMultipleorIfProxy = process.env.CI_PROXY === 'true' ?
    describe.skip : describeSkipIfNotMultiple;

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
        assert((res && res.Contents &&
            res.Contents.length === objCount));
        res.Contents.forEach(part => {
            assert.strictEqual(
                part.ETag, `"${expected}"`);
        });
        cb();
    });
}

skipIfNotMultipleorIfProxy('MultipleBacked put part to GCP', function
describeFn() {
    this.timeout(180000);
    withV4(sigCfg => {
        beforeEach(function beforeFn() {
            this.currentTest.key = uniqName(keyObject);
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
        });

        describe('with bucket location header', () => {
            beforeEach(function beforeEachFn(done) {
                async.waterfall([
                    next => s3.createBucket({ Bucket: bucket,
                    }, err => next(err)),
                    next => s3.createMultipartUpload({
                        Bucket: bucket,
                        Key: this.currentTest.key,
                        Metadata: { 'scal-location-constraint': gcpLocation },
                    }, (err, res) => {
                        if (err) {
                            return next(err);
                        }
                        this.currentTest.uploadId = res.UploadId;
                        return next();
                    }),
                ], done);
            });

            afterEach(function afterEachFn(done) {
                async.waterfall([
                    next => s3.abortMultipartUpload({
                        Bucket: bucket,
                        Key: this.currentTest.key,
                        UploadId: this.currentTest.uploadId,
                    }, err => next(err)),
                    next => s3.deleteBucket({ Bucket: bucket },
                      err => next(err)),
                ], err => {
                    assert.equal(err, null, `Error aborting MPU: ${err}`);
                    done();
                });
            });

            it('should put 0-byte part to GCP', function itFn(done) {
                const params = {
                    Bucket: bucket,
                    Key: this.test.key,
                    UploadId: this.test.uploadId,
                    PartNumber: 1,
                };
                async.waterfall([
                    next => s3.uploadPart(params, (err, res) => {
                        assert.ifError(err,
                            `Expected success, but got err ${err}`);
                        assert.strictEqual(res.ETag, `"${emptyMD5}"`);
                        next();
                    }),
                    next => {
                        const mpuKey =
                            createMpuKey(this.test.key, this.test.uploadId, 1);
                        const getParams = {
                            Bucket: gcpBucketMPU,
                            Key: mpuKey,
                        };
                        gcpClient.getObject(getParams, (err, res) => {
                            assert.ifError(err,
                                `Expected success, but got err ${err}`);
                            assert.strictEqual(res.ETag, `"${emptyMD5}"`);
                            next();
                        });
                    },
                ], done);
            });

            it('should put 2 parts to GCP', function ifFn(done) {
                async.waterfall([
                    next => {
                        async.times(2, (n, cb) => {
                            const params = {
                                Bucket: bucket,
                                Key: this.test.key,
                                UploadId: this.test.uploadId,
                                Body: body,
                                PartNumber: n + 1,
                            };
                            s3.uploadPart(params, (err, res) => {
                                assert.ifError(err,
                                    `Expected success, but got err ${err}`);
                                assert.strictEqual(
                                    res.ETag, `"${correctMD5}"`);
                                cb();
                            });
                        }, () => next());
                    },
                    next => checkMPUResult(
                        gcpBucketMPU, this.test.key, this.test.uploadId,
                        2, correctMD5, next),
                ], done);
            });

            it('should put the same part twice', function ifFn(done) {
                async.waterfall([
                    next => {
                        const partBody = ['', body];
                        const partMD5 = [emptyMD5, correctMD5];
                        async.timesSeries(2, (n, cb) => {
                            const params = {
                                Bucket: bucket,
                                Key: this.test.key,
                                UploadId: this.test.uploadId,
                                Body: partBody[n],
                                PartNumber: 1,
                            };
                            s3.uploadPart(params, (err, res) => {
                                assert.ifError(err,
                                    `Expected success, but got err ${err}`);
                                assert.strictEqual(
                                    res.ETag, `"${partMD5[n]}"`);
                                cb();
                            });
                        }, () => next());
                    },
                    next => checkMPUResult(
                        gcpBucketMPU, this.test.key, this.test.uploadId,
                        1, correctMD5, next),
                ], done);
            });
        });

        describe('with same key as preexisting part', () => {
            beforeEach(function beforeEachFn(done) {
                async.waterfall([
                    next => s3.createBucket({ Bucket: bucket },
                        err => next(err)),
                    next => {
                        s3.putObject({
                            Bucket: bucket,
                            Key: this.currentTest.key,
                            Metadata: {
                                'scal-location-constraint': gcpLocation },
                            Body: body,
                        }, err => {
                            assert.equal(err, null, 'Err putting object to ' +
                            `GCP: ${err}`);
                            return next();
                        });
                    },
                    next => s3.createMultipartUpload({
                        Bucket: bucket,
                        Key: this.currentTest.key,
                        Metadata: { 'scal-location-constraint': gcpLocation },
                    }, (err, res) => {
                        if (err) {
                            return next(err);
                        }
                        this.currentTest.uploadId = res.UploadId;
                        return next();
                    }),
                ], done);
            });

            afterEach(function afterEachFn(done) {
                async.waterfall([
                    next => {
                        process.stdout.write('Aborting multipart upload\n');
                        s3.abortMultipartUpload({
                            Bucket: bucket,
                            Key: this.currentTest.key,
                            UploadId: this.currentTest.uploadId },
                        err => next(err));
                    },
                    next => {
                        process.stdout.write('Deleting object\n');
                        s3.deleteObject({
                            Bucket: bucket,
                            Key: this.currentTest.key },
                        err => next(err));
                    },
                    next => {
                        process.stdout.write('Deleting bucket\n');
                        s3.deleteBucket({
                            Bucket: bucket },
                        err => next(err));
                    },
                ], err => {
                    assert.equal(err, null, `Err in afterEach: ${err}`);
                    done();
                });
            });

            it('should put a part without overwriting existing object',
            function itFn(done) {
                const body = Buffer.alloc(20);
                s3.uploadPart({
                    Bucket: bucket,
                    Key: this.test.key,
                    UploadId: this.test.uploadId,
                    PartNumber: 1,
                    Body: body,
                }, err => {
                    assert.strictEqual(err, null, 'Err putting part to ' +
                    `GCP: ${err}`);
                    gcpClient.getObject({
                        Bucket: gcpBucket,
                        Key: this.test.key,
                    }, (err, res) => {
                        assert.ifError(err,
                            `Expected success, but got err ${err}`);
                        assert.strictEqual(res.ETag, `"${correctMD5}"`);
                        done();
                    });
                });
            });
        });
    });
});

describeSkipIfNotMultiple('MultipleBackend put part to GCP location with ' +
'bucketMatch sets to false', function
describeF() {
    this.timeout(80000);
    withV4(sigCfg => {
        beforeEach(function beforeFn() {
            this.currentTest.key = uniqName(keyObject);
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
        });
        describe('with bucket location header', () => {
            beforeEach(function beforeEachFn(done) {
                async.waterfall([
                    next => s3.createBucket({ Bucket: bucket,
                    }, err => next(err)),
                    next => s3.createMultipartUpload({
                        Bucket: bucket,
                        Key: this.currentTest.key,
                        Metadata: { 'scal-location-constraint':
                        gcpLocationMismatch },
                    }, (err, res) => {
                        if (err) {
                            return next(err);
                        }
                        this.currentTest.uploadId = res.UploadId;
                        return next();
                    }),
                ], done);
            });

            afterEach(function afterEachFn(done) {
                async.waterfall([
                    next => s3.abortMultipartUpload({
                        Bucket: bucket,
                        Key: this.currentTest.key,
                        UploadId: this.currentTest.uploadId,
                    }, err => next(err)),
                    next => s3.deleteBucket({ Bucket: bucket },
                      err => next(err)),
                ], err => {
                    assert.equal(err, null, `Error aborting MPU: ${err}`);
                    done();
                });
            });

            it('should put part to GCP location with bucketMatch' +
            ' sets to false', function itFn(done) {
                const body20 = Buffer.alloc(20);
                const params = {
                    Bucket: bucket,
                    Key: this.test.key,
                    UploadId: this.test.uploadId,
                    PartNumber: 1,
                    Body: body20,
                };
                const eTagExpected =
                    '"441018525208457705bf09a8ee3c1093"';
                async.waterfall([
                    next => s3.uploadPart(params, (err, res) => {
                        assert.strictEqual(res.ETag, eTagExpected);
                        next(err);
                    }),
                    next => {
                        const key =
                            createMpuKey(this.test.key, this.test.uploadId, 1);
                        const mpuKey = `${bucket}/${key}`;
                        const getParams = {
                            Bucket: gcpBucketMPU,
                            Key: mpuKey,
                        };
                        gcpClient.getObject(getParams, (err, res) => {
                            assert.ifError(err,
                                `Expected success, but got err ${err}`);
                            assert.strictEqual(res.ETag, eTagExpected);
                            next();
                        });
                    },
                ], done);
            });
        });
    });
});
