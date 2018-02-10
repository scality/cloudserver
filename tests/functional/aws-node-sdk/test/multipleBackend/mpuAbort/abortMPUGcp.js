const assert = require('assert');
const async = require('async');

const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const { describeSkipIfNotMultiple, gcpClient, gcpBucket, gcpBucketMPU,
    gcpLocation, uniqName } = require('../utils');

const keyObject = 'abortgcp';
const bucket = 'buckettestmultiplebackendabortmpu-gcp';
const body = Buffer.from('I am a body', 'utf8');
const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';

let bucketUtil;
let s3;

function checkMPUList(bucket, key, uploadId, cb) {
    const params = {
        Bucket: bucket,
        Key: key,
        UploadId: uploadId,
    };
    gcpClient.listParts(params, (err, res) => {
        assert.ifError(err,
            `Expected success, but got err ${err}`);
        assert.deepStrictEqual(res.Contents, [],
            'Expected 0 parts, listed some');
        cb();
    });
}

describeSkipIfNotMultiple('Abort MPU on GCP data backend', function
descrbeFn() {
    this.timeout(50000);
    withV4(sigCfg => {
        beforeEach(function beforeFn() {
            this.currentTest.key = uniqName(keyObject);
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
        });

        describe('with bucket location header', () => {
            beforeEach(function beforeEachFn(done) {
                async.waterfall([
                    next => s3.createBucket({ Bucket: bucket },
                        err => next(err)),
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

            afterEach(done => s3.deleteBucket({ Bucket: bucket },
                done));

            it('should abort a MPU with 0 parts', function itFn(done) {
                const params = {
                    Bucket: bucket,
                    Key: this.test.key,
                    UploadId: this.test.uploadId,
                };
                async.waterfall([
                    next => s3.abortMultipartUpload(params, () => next()),
                    next => checkMPUList(
                        gcpBucketMPU, this.test.key, this.test.uploadId, next),
                ], done);
            });

            it('should abort a MPU with uploaded parts', function itFn(done) {
                const params = {
                    Bucket: bucket,
                    Key: this.test.key,
                    UploadId: this.test.uploadId,
                };
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
                    next => s3.abortMultipartUpload(params, () => next()),
                    next => checkMPUList(
                        gcpBucketMPU, this.test.key, this.test.uploadId, next),
                ], done);
            });
        });

        describe('with previously existing object with same key', () => {
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
                            assert.ifError(err,
                                `Expected success, got error: ${err}`);
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

            afterEach(() => {
                process.stdout.write('Emptying bucket\n');
                return bucketUtil.empty(bucket)
                .then(() => {
                    process.stdout.write('Deleting bucket\n');
                    return bucketUtil.deleteOne(bucket);
                })
                .catch(err => {
                    process.stdout.write('Error emptying/deleting bucket: ' +
                    `${err}\n`);
                    throw err;
                });
            });

            it('should abort MPU without deleting existing object',
            function itFn(done) {
                const params = {
                    Bucket: bucket,
                    Key: this.test.key,
                    UploadId: this.test.uploadId,
                };
                async.waterfall([
                    next => {
                        const body = Buffer.alloc(10);
                        const partParams = Object.assign(
                            { PartNumber: 1, Body: body }, params);
                        s3.uploadPart(partParams, err => {
                            assert.ifError(err,
                                `Expected success, got error: ${err}`);
                            return next();
                        });
                    },
                    next => s3.abortMultipartUpload(params, () => next()),
                    next => {
                        const params = {
                            Bucket: gcpBucket,
                            Key: this.test.key,
                        };
                        gcpClient.getObject(params, (err, res) => {
                            assert.ifError(err,
                                `Expected success, got error: ${err}`);
                            assert.strictEqual(res.ETag, `"${correctMD5}"`);
                            next();
                        });
                    },
                ], done);
            });
        });
    });
});
