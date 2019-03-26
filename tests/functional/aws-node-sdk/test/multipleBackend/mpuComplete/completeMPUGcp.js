const assert = require('assert');
const async = require('async');

const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const { describeSkipIfNotMultipleOrCeph, fileLocation, awsS3, awsLocation,
    awsBucket, gcpClient, gcpBucket, gcpLocation, gcpLocationMismatch,
    genUniqID } = require('../utils');

const bucket = `completempugcp${genUniqID()}`;
const smallBody = Buffer.from('I am a body', 'utf8');
const bigBody = Buffer.alloc(10485760);
const s3MD5 = 'bfb875032e51cbe2a60c5b6b99a2153f-2';
const expectedContentLength = '10485771';
const gcpTimeout = 5000;

let s3;
let bucketUtil;

function getCheck(key, bucketMatch, cb) {
    let gcpKey = key;
    s3.getObject({ Bucket: bucket, Key: gcpKey },
    (err, s3Res) => {
        expect(err).toEqual(null);
        expect(s3Res.ETag).toBe(`"${s3MD5}"`);

        if (!bucketMatch) {
            gcpKey = `${bucket}/${gcpKey}`;
        }
        const params = { Bucket: gcpBucket, Key: gcpKey };
        gcpClient.getObject(params, (err, gcpRes) => {
            expect(err).toEqual(null);
            expect(expectedContentLength).toBe(gcpRes.ContentLength);
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
                expect(uploadId).toBeTruthy();
                expect(res.Bucket).toBe(bucket);
                expect(res.Key).toBe(key);
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
        expect(err).toEqual(null);
        cb(uploadId, partArray);
    });
}

describeSkipIfNotMultipleOrCeph('Complete MPU API for GCP data backend',
function testSuite() {
    this.timeout(150000);
    withV4(sigCfg => {
        beforeEach(() => {
            this.currentTest.key = `somekey-${genUniqID()}`;
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

        test('should complete an MPU on GCP', done => {
            mpuSetup(this.test.key, gcpLocation, (uploadId, partArray) => {
                const params = {
                    Bucket: bucket,
                    Key: this.test.key,
                    UploadId: uploadId,
                    MultipartUpload: { Parts: partArray },
                };
                setTimeout(() => {
                    s3.completeMultipartUpload(params, err => {
                        expect(err).toEqual(null);
                        getCheck(this.test.key, true, done);
                    });
                }, gcpTimeout);
            });
        });

        test(
            'should complete an MPU on GCP with bucketMatch=false',
            done => {
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
                            expect(err).toEqual(null);
                            getCheck(this.test.key, false, done);
                        });
                    }, gcpTimeout);
                });
            }
        );

        test('should complete an MPU on GCP with same key as object put ' +
        'to file', done => {
            const body = Buffer.from('I am a body', 'utf8');
            s3.putObject({
                Bucket: bucket,
                Key: this.test.key,
                Body: body,
                Metadata: { 'scal-location-constraint': fileLocation } },
            err => {
                expect(err).toEqual(null);
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
                            expect(err).toEqual(null);
                            getCheck(this.test.key, true, done);
                        });
                    }, gcpTimeout);
                });
            });
        });

        test('should complete an MPU on GCP with same key as object put ' +
        'to GCP', done => {
            const body = Buffer.from('I am a body', 'utf8');
            s3.putObject({
                Bucket: bucket,
                Key: this.test.key,
                Body: body,
                Metadata: { 'scal-location-constraint': gcpLocation } },
            err => {
                expect(err).toEqual(null);
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
                            expect(err).toEqual(null);
                            getCheck(this.test.key, true, done);
                        });
                    }, gcpTimeout);
                });
            });
        });

        test('should complete an MPU on GCP with same key as object put ' +
        'to AWS', done => {
            const body = Buffer.from('I am a body', 'utf8');
            s3.putObject({
                Bucket: bucket,
                Key: this.test.key,
                Body: body,
                Metadata: { 'scal-location-constraint': awsLocation } },
            err => {
                expect(err).toEqual(null);
                mpuSetup(this.test.key, gcpLocation,
                (uploadId, partArray) => {
                    const params = {
                        Bucket: bucket,
                        Key: this.test.key,
                        UploadId: uploadId,
                        MultipartUpload: { Parts: partArray },
                    };
                    s3.completeMultipartUpload(params, err => {
                        expect(err).toEqual(null);
                        // make sure object is gone from AWS
                        setTimeout(() => {
                            this.test.awsClient.getObject({ Bucket: awsBucket,
                            Key: this.test.key }, err => {
                                expect(err.code).toBe('NoSuchKey');
                                getCheck(this.test.key, true, done);
                            });
                        }, gcpTimeout);
                    });
                });
            });
        });
    });
});
