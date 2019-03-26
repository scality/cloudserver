const assert = require('assert');
const async = require('async');

const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const { describeSkipIfNotMultipleOrCeph, gcpClient, gcpBucket, gcpBucketMPU,
    gcpLocation, uniqName, genUniqID } = require('../utils');

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
        assert.ifError(err,
            `Expected success, but got err ${err}`);
        assert.deepStrictEqual(res.Contents, [],
            'Expected 0 parts, listed some');
        cb();
    });
}

describeSkipIfNotMultipleOrCeph('Abort MPU on GCP data backend', function
descrbeFn() {
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
                    next => s3.createBucket({ Bucket: bucket },
                        err => next(err)),
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

            afterEach(done => s3.deleteBucket({ Bucket: bucket },
                done));

            test('should abort a MPU with 0 parts', done => {
                const params = {
                    Bucket: bucket,
                    Key: testContext.test.key,
                    UploadId: testContext.test.uploadId,
                };
                async.waterfall([
                    next => s3.abortMultipartUpload(params, () => next()),
                    next => setTimeout(() => checkMPUList(
                        gcpBucketMPU, testContext.test.key, testContext.test.uploadId, next),
                    gcpTimeout),
                ], done);
            });

            test('should abort a MPU with uploaded parts', done => {
                const params = {
                    Bucket: bucket,
                    Key: testContext.test.key,
                    UploadId: testContext.test.uploadId,
                };
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
                    next => s3.abortMultipartUpload(params, () => next()),
                    next => setTimeout(() => checkMPUList(
                        gcpBucketMPU, testContext.test.key, testContext.test.uploadId, next),
                    gcpTimeout),
                ], done);
            });
        });

        describe('with previously existing object with same key', () => {
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
                            assert.ifError(err,
                                `Expected success, got error: ${err}`);
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

            test('should abort MPU without deleting existing object', done => {
                const params = {
                    Bucket: bucket,
                    Key: testContext.test.key,
                    UploadId: testContext.test.uploadId,
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
                    next => setTimeout(() => {
                        const params = {
                            Bucket: gcpBucket,
                            Key: testContext.test.key,
                        };
                        gcpClient.getObject(params, (err, res) => {
                            assert.ifError(err,
                                `Expected success, got error: ${err}`);
                            expect(res.ETag).toBe(`"${correctMD5}"`);
                            next();
                        });
                    }, gcpTimeout),
                ], done);
            });
        });
    });
});
