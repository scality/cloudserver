const assert = require('assert');
const async = require('async');
const {
    CreateBucketCommand,
    DeleteBucketCommand,
    CreateMultipartUploadCommand,
    UploadPartCommand,
    AbortMultipartUploadCommand,
    PutObjectCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const {
    gcpClient,
    gcpBucket,
    gcpBucketMPU,
    gcpLocation,
    uniqName,
    genUniqID,
    describeSkipIfNotMultiple,
} = require('../utils');

const keyObject = 'abortgcp';
const bucket = `abortmpugcp${genUniqID()}`;
const body = Buffer.from('I am a body', 'utf8');
const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
const gcpTimeout = 5000;

let bucketUtil;
let s3;

function checkMPUList(bucket, key, uploadId, cb) {
    const params = {
        Bucket: bucket,
        Key: key,
        UploadId: uploadId,
    };
    gcpClient.listParts(params, (err, res) => {
        assert.ifError(err, `Expected success, but got err ${err}`);
        assert.deepStrictEqual(res.Contents, [], 'Expected 0 parts, listed some');
        cb();
    });
}

describeSkipIfNotMultiple('Abort MPU on GCP data backend', function descrbeFn() {
    this.timeout(180000);
    withV4(sigCfg => {
        beforeEach(function beforeFn() {
            this.currentTest.key = uniqName(keyObject);
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
        });

        describe('with bucket location header', () => {
            beforeEach(function beforeEachFn(done) {
                async.waterfall(
                    [
                        next => {
                            s3.send(
                                new CreateBucketCommand({
                                    Bucket: bucket,
                                }),
                            )
                                .then(() => next())
                                .catch(next);
                        },
                        next => {
                            s3.send(
                                new CreateMultipartUploadCommand({
                                    Bucket: bucket,
                                    Key: this.currentTest.key,
                                    Metadata: { 'scal-location-constraint': gcpLocation },
                                }),
                            )
                                .then(res => {
                                    this.currentTest.uploadId = res.UploadId;
                                    next();
                                })
                                .catch(next);
                        },
                    ],
                    done,
                );
            });

            afterEach(done => {
                s3.send(
                    new DeleteBucketCommand({
                        Bucket: bucket,
                    }),
                )
                    .then(() => done())
                    .catch(done);
            });

            it('should abort a MPU with 0 parts', function itFn(done) {
                const params = {
                    Bucket: bucket,
                    Key: this.test.key,
                    UploadId: this.test.uploadId,
                };
                async.waterfall(
                    [
                        next => {
                            s3.send(new AbortMultipartUploadCommand(params))
                                .then(() => next())
                                .catch(next);
                        },
                        next =>
                            setTimeout(
                                () => checkMPUList(gcpBucketMPU, this.test.key, this.test.uploadId, next),
                                gcpTimeout,
                            ),
                    ],
                    done,
                );
            });

            it('should abort a MPU with uploaded parts', function itFn(done) {
                const params = {
                    Bucket: bucket,
                    Key: this.test.key,
                    UploadId: this.test.uploadId,
                };
                async.waterfall(
                    [
                        next => {
                            async.times(
                                2,
                                (n, cb) => {
                                    const uploadParams = {
                                        Bucket: bucket,
                                        Key: this.test.key,
                                        UploadId: this.test.uploadId,
                                        Body: body,
                                        PartNumber: n + 1,
                                    };
                                    s3.send(new UploadPartCommand(uploadParams))
                                        .then(res => {
                                            assert.strictEqual(res.ETag, `"${correctMD5}"`);
                                            cb();
                                        })
                                        .catch(err => {
                                            assert.ifError(err, `Expected success, but got err ${err}`);
                                            cb(err);
                                        });
                                },
                                err => next(err),
                            );
                        },
                        next => {
                            s3.send(new AbortMultipartUploadCommand(params))
                                .then(() => next())
                                .catch(next);
                        },
                        next =>
                            setTimeout(
                                () => checkMPUList(gcpBucketMPU, this.test.key, this.test.uploadId, next),
                                gcpTimeout,
                            ),
                    ],
                    done,
                );
            });
        });

        describe('with previously existing object with same key', () => {
            beforeEach(function beforeEachFn(done) {
                async.waterfall(
                    [
                        next => {
                            s3.send(
                                new CreateBucketCommand({
                                    Bucket: bucket,
                                }),
                            )
                                .then(() => next())
                                .catch(next);
                        },
                        next => {
                            s3.send(
                                new PutObjectCommand({
                                    Bucket: bucket,
                                    Key: this.currentTest.key,
                                    Metadata: {
                                        'scal-location-constraint': gcpLocation,
                                    },
                                    Body: body,
                                }),
                            )
                                .then(() => next())
                                .catch(err => {
                                    assert.ifError(err, `Expected success, got error: ${err}`);
                                    next(err);
                                });
                        },
                        next => {
                            s3.send(
                                new CreateMultipartUploadCommand({
                                    Bucket: bucket,
                                    Key: this.currentTest.key,
                                    Metadata: { 'scal-location-constraint': gcpLocation },
                                }),
                            )
                                .then(res => {
                                    this.currentTest.uploadId = res.UploadId;
                                    next();
                                })
                                .catch(next);
                        },
                    ],
                    done,
                );
            });

            afterEach(() => {
                process.stdout.write('Emptying bucket\n');
                return bucketUtil
                    .empty(bucket)
                    .then(() => {
                        process.stdout.write('Deleting bucket\n');
                        return bucketUtil.deleteOne(bucket);
                    })
                    .catch(err => {
                        process.stdout.write('Error emptying/deleting bucket: ' + `${err}\n`);
                        throw err;
                    });
            });

            it('should abort MPU without deleting existing object', function itFn(done) {
                const params = {
                    Bucket: bucket,
                    Key: this.test.key,
                    UploadId: this.test.uploadId,
                };
                async.waterfall(
                    [
                        next => {
                            const body = Buffer.alloc(10);
                            const partParams = Object.assign({ PartNumber: 1, Body: body }, params);
                            s3.send(new UploadPartCommand(partParams))
                                .then(() => next())
                                .catch(err => {
                                    assert.ifError(err, `Expected success, got error: ${err}`);
                                    next(err);
                                });
                        },
                        next => {
                            s3.send(new AbortMultipartUploadCommand(params))
                                .then(() => next())
                                .catch(next);
                        },
                        next =>
                            setTimeout(() => {
                                const params = {
                                    Bucket: gcpBucket,
                                    Key: this.test.key,
                                };
                                gcpClient.getObject(params, (err, res) => {
                                    assert.ifError(err, `Expected success, got error: ${err}`);
                                    assert.strictEqual(res.ETag, `"${correctMD5}"`);
                                    next();
                                });
                            }, gcpTimeout),
                    ],
                    done,
                );
            });
        });
    });
});
