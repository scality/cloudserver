import assert from 'assert';

import { S3 } from 'aws-sdk';
import { timesLimit, waterfall } from 'async';

import getConfig from '../support/config';

const bucket = `bigmpu-test-bucket-${Date.now()}`;
const key = 'mpuKey';
const body = 'abc';
const eTag = '900150983cd24fb0d6963f7d28e17f72';

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

describe.only('large mpu', function tester() {
    this.timeout(600000);
    let s3;
    before(done => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3(config);
        // normal retry base is 100, want see if can duplicate test
        // failure by reducing
        s3.config.update({ retryDelayOptions: { base: 50 } });
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
        'with 10,000 parts', done => {
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
                return timesLimit(10000, 20, (n, cb) =>
                    uploadPart(n, uploadId, s3, cb), err =>
                        next(err)
                    );
            },
            next => {
                const parts = [];
                for (let i = 1; i <= 10000; i++) {
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
                const timeStart = Date.now();
                return s3.completeMultipartUpload(params, err => {
                    const timeElapsed = Date.now() - timeStart;
                    console.log(`completempu time elapsed: ${timeElapsed} ms`);
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
                        assert.strictEqual(data.ETag, '"e0c3d6b4446bf8f97' +
                            '9c50df6d79e9e0a-10000"');
                        return next();
                    });
            },
        ], done);
    });
});
