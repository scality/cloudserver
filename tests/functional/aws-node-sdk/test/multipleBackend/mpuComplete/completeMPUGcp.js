const assert = require('assert');
const async = require('async');

const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const { describeSkipIfNotMultiple, fileLocation, awsS3, awsLocation, awsBucket,
    gcpClient, gcpBucket, gcpLocation, gcpLocationMismatch } =
    require('../utils');

const bucket = 'buckettestmultiplebackendcompletempu-gcp';
const smallBody = Buffer.from('I am a body', 'utf8');
const bigBody = Buffer.alloc(10485760);
const s3MD5 = 'bfb875032e51cbe2a60c5b6b99a2153f-2';
const expectedContentLength = '10485771';
const gcpTimeout = 5000;
const skipIfNotMultipleorIfProxy = process.env.CI_PROXY === 'true' ?
    describe.skip : describeSkipIfNotMultiple;

let s3;
let bucketUtil;

function getCheck(key, bucketMatch, cb) {
    let gcpKey = key;
    s3.getObject({ Bucket: bucket, Key: gcpKey },
    (err, s3Res) => {
        assert.equal(err, null, `Err getting object from S3: ${err}`);
        assert.strictEqual(s3Res.ETag, `"${s3MD5}"`);

        if (!bucketMatch) {
            gcpKey = `${bucket}/${gcpKey}`;
        }
        const params = { Bucket: gcpBucket, Key: gcpKey };
        gcpClient.getObject(params, (err, gcpRes) => {
            assert.equal(err, null, `Err getting object from GCP: ${err}`);
            assert.strictEqual(expectedContentLength, gcpRes.ContentLength);
            cb();
        });
    });
}

function mpuSetup(key, location, cb) {
    const partArray = [];
    async.waterfall([
        next => {
            const params = {
                Bucket: bucket,
                Key: key,
                Metadata: { 'scal-location-constraint': location },
            };
            s3.createMultipartUpload(params, (err, res) => {
                const uploadId = res.UploadId;
                assert(uploadId);
                assert.strictEqual(res.Bucket, bucket);
                assert.strictEqual(res.Key, key);
                next(err, uploadId);
            });
        },
        (uploadId, next) => {
            const partParams = {
                Bucket: bucket,
                Key: key,
                PartNumber: 1,
                UploadId: uploadId,
                Body: smallBody,
            };
            s3.uploadPart(partParams, (err, res) => {
                partArray.push({ ETag: res.ETag, PartNumber: 1 });
                next(err, uploadId);
            });
        },
        (uploadId, next) => {
            const partParams = {
                Bucket: bucket,
                Key: key,
                PartNumber: 2,
                UploadId: uploadId,
                Body: bigBody,
            };
            s3.uploadPart(partParams, (err, res) => {
                partArray.push({ ETag: res.ETag, PartNumber: 2 });
                next(err, uploadId);
            });
        },
    ], (err, uploadId) => {
        process.stdout.write('Created MPU and put two parts\n');
        assert.equal(err, null, `Err setting up MPU: ${err}`);
        cb(uploadId, partArray);
    });
}

skipIfNotMultipleorIfProxy('Complete MPU API for GCP data backend',
function testSuite() {
    this.timeout(150000);
    withV4(sigCfg => {
        beforeEach(function beFn() {
            this.currentTest.key = `somekey-${Date.now()}`;
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            this.currentTest.awsClient = awsS3;
            return s3.createBucketAsync({ Bucket: bucket })
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            });
        });

        afterEach(() => {
            process.stdout.write('Emptying bucket\n');
            return bucketUtil.empty(bucket)
            .then(() => {
                process.stdout.write('Deleting bucket\n');
                return bucketUtil.deleteOne(bucket);
            })
            .catch(err => {
                process.stdout.write(`Error in afterEach: ${err}\n`);
                throw err;
            });
        });

        it('should complete an MPU on GCP', function itFn(done) {
            mpuSetup(this.test.key, gcpLocation, (uploadId, partArray) => {
                const params = {
                    Bucket: bucket,
                    Key: this.test.key,
                    UploadId: uploadId,
                    MultipartUpload: { Parts: partArray },
                };
                setTimeout(() => {
                    s3.completeMultipartUpload(params, err => {
                        assert.equal(err, null,
                            `Err completing MPU: ${err}`);
                        getCheck(this.test.key, true, done);
                    });
                }, gcpTimeout);
            });
        });

        it('should complete an MPU on GCP with bucketMatch=false',
        function itFn(done) {
            mpuSetup(this.test.key, gcpLocationMismatch,
            (uploadId, partArray) => {
                const params = {
                    Bucket: bucket,
                    Key: this.test.key,
                    UploadId: uploadId,
                    MultipartUpload: { Parts: partArray },
                };
                setTimeout(() => {
                    s3.completeMultipartUpload(params, err => {
                        assert.equal(err, null,
                            `Err completing MPU: ${err}`);
                        getCheck(this.test.key, false, done);
                    });
                }, gcpTimeout);
            });
        });

        it('should complete an MPU on GCP with same key as object put ' +
        'to file', function itFn(done) {
            const body = Buffer.from('I am a body', 'utf8');
            s3.putObject({
                Bucket: bucket,
                Key: this.test.key,
                Body: body,
                Metadata: { 'scal-location-constraint': fileLocation } },
            err => {
                assert.equal(err, null, `Err putting object to file: ${err}`);
                mpuSetup(this.test.key, gcpLocation,
                (uploadId, partArray) => {
                    const params = {
                        Bucket: bucket,
                        Key: this.test.key,
                        UploadId: uploadId,
                        MultipartUpload: { Parts: partArray },
                    };
                    setTimeout(() => {
                        s3.completeMultipartUpload(params, err => {
                            assert.equal(err, null,
                                `Err completing MPU: ${err}`);
                            getCheck(this.test.key, true, done);
                        });
                    }, gcpTimeout);
                });
            });
        });

        it('should complete an MPU on GCP with same key as object put ' +
        'to GCP', function itFn(done) {
            const body = Buffer.from('I am a body', 'utf8');
            s3.putObject({
                Bucket: bucket,
                Key: this.test.key,
                Body: body,
                Metadata: { 'scal-location-constraint': gcpLocation } },
            err => {
                assert.equal(err, null, `Err putting object to GCP: ${err}`);
                mpuSetup(this.test.key, gcpLocation,
                (uploadId, partArray) => {
                    const params = {
                        Bucket: bucket,
                        Key: this.test.key,
                        UploadId: uploadId,
                        MultipartUpload: { Parts: partArray },
                    };
                    setTimeout(() => {
                        s3.completeMultipartUpload(params, err => {
                            assert.equal(err, null,
                                `Err completing MPU: ${err}`);
                            getCheck(this.test.key, true, done);
                        });
                    }, gcpTimeout);
                });
            });
        });

        it('should complete an MPU on GCP with same key as object put ' +
        'to AWS', function itFn(done) {
            const body = Buffer.from('I am a body', 'utf8');
            s3.putObject({
                Bucket: bucket,
                Key: this.test.key,
                Body: body,
                Metadata: { 'scal-location-constraint': awsLocation } },
            err => {
                assert.equal(err, null, `Err putting object to AWS: ${err}`);
                mpuSetup(this.test.key, gcpLocation,
                (uploadId, partArray) => {
                    const params = {
                        Bucket: bucket,
                        Key: this.test.key,
                        UploadId: uploadId,
                        MultipartUpload: { Parts: partArray },
                    };
                    s3.completeMultipartUpload(params, err => {
                        assert.equal(err, null, `Err completing MPU: ${err}`);
                        // make sure object is gone from AWS
                        setTimeout(() => {
                            this.test.awsClient.getObject({ Bucket: awsBucket,
                            Key: this.test.key }, err => {
                                assert.strictEqual(err.code, 'NoSuchKey');
                                getCheck(this.test.key, true, done);
                            });
                        }, gcpTimeout);
                    });
                });
            });
        });
    });
});
