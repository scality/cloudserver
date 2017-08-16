const assert = require('assert');

const { S3 } = require('aws-sdk');
const { timesLimit, waterfall } = require('async');

const getConfig = require('../support/config');

const bucket = `bigmpu-test-bucket-${Date.now()}`;
const key = 'mpuKey';
const body = 'abc';
const partCount = 10000;
const eTag = require('crypto').createHash('md5').update(body).digest('hex');
const finalETag = require('crypto').createHash('md5')
    .update(Buffer.from(eTag.repeat(partCount), 'hex').toString('binary'),
            'binary').digest('hex');

function uploadPart(n, uploadId, s3, next) {
    const params = {
        Bucket: bucket,
        Key: key,
        PartNumber: n + 1,
        UploadId: uploadId,
        Body: body,
    };
    if (params.PartNumber % 20 === 0) {
        process.stdout.write(`uploading PartNumber: ${params.PartNumber}\n`);
    }
    s3.uploadPart(params, err => {
        if (err) {
            process.stdout.write('error putting part: ', err);
            return next(err);
        }
        return next();
    });
}

// NOTE: This test has a history of failing in end-to-end Integration tests.
// See Integration#449 for more details. A possible cause for its flakiness
// could be poor system resources.
describe('large mpu', function tester() {
    this.timeout(600000);
    let s3;
    before(done => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3(config);
        // disable node sdk retries and timeout to prevent InvalidPart
        // and SocketHangUp errors. If retries are allowed, sdk will send
        // another request after first request has already deleted parts,
        // causing InvalidPart. Meanwhile, if request takes too long to finish,
        // sdk will create SocketHangUp error before response.
        s3.config.update({ maxRetries: 0 });
        s3.config.update({ httpOptions: { timeout: 0 } });
        s3.createBucket({ Bucket: bucket }, done);
    });

    after(done => {
        s3.deleteObject({ Bucket: bucket, Key: key }, err => {
            if (err) {
                process.stdout.write('err deleting object in after: ', err);
                return done(err);
            }
            return s3.deleteBucket({ Bucket: bucket }, done);
        });
    });

    const itSkipIfAWS = process.env.AWS_ON_AIR ? it.skip : it;
    // will fail on AWS because parts too small

    itSkipIfAWS('should intiate, put parts and complete mpu ' +
        `with ${partCount} parts`, done => {
        process.stdout.write('***Running large MPU test***\n');
        let uploadId;
        return waterfall([
            next => s3.createMultipartUpload({ Bucket: bucket, Key: key },
                (err, data) => {
                    if (err) {
                        return done(err);
                    }
                    process.stdout.write('initated mpu\n');
                    uploadId = data.UploadId;
                    return next();
                }),
            next => {
                process.stdout.write('putting parts');
                return timesLimit(partCount, 20, (n, cb) =>
                    uploadPart(n, uploadId, s3, cb), err =>
                        next(err)
                    );
            },
            next => {
                const parts = [];
                for (let i = 1; i <= partCount; i++) {
                    parts.push({
                        ETag: eTag,
                        PartNumber: i,
                    });
                }
                const params = {
                    Bucket: bucket,
                    Key: key,
                    UploadId: uploadId,
                    MultipartUpload: {
                        Parts: parts,
                    },
                };
                return s3.completeMultipartUpload(params, err => {
                    if (err) {
                        process.stdout.write('err complting mpu: ', err);
                        return next(err);
                    }
                    return next();
                });
            },
            next => {
                process.stdout.write('about to get object');
                return s3.getObject({ Bucket: bucket, Key: key },
                    (err, data) => {
                        if (err) {
                            return next(err);
                        }
                        assert.strictEqual(data.ETag,
                                `"${finalETag}-${partCount}"`);
                        return next();
                    });
            },
        ], done);
    });
});
