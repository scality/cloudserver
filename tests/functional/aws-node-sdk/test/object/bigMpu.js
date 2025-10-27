const assert = require('assert');
const { timesLimit, waterfall } = require('async');
const { NodeHttpHandler } = require('@smithy/node-http-handler');

const { 
    S3Client,
    CreateBucketCommand,
    CreateMultipartUploadCommand, 
    UploadPartCommand, 
    CompleteMultipartUploadCommand, 
    GetObjectCommand,
    DeleteObjectCommand,
    DeleteBucketCommand
} = require('@aws-sdk/client-s3');

const getConfig = require('../support/config');

const bucket = `bigmpu-test-bucket-${Date.now()}`;
const key = 'mpuKey';
const body = 'abc';
const partCount = 10000;
const eTag = require('crypto').createHash('md5').update(body).digest('hex');
const finalETag = require('crypto').createHash('md5')
    .update(Buffer.from(eTag.repeat(partCount), 'hex').toString('binary'),
            'binary').digest('hex');

const partETags = new Array(partCount);
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
    
    s3.send(new UploadPartCommand(params))
        .then(data => {
            partETags[n] = data.ETag;
            next();
        })
        .catch(err => {
            process.stdout.write(`error putting part ${params.PartNumber}: ${err}\n`);
            return next(err);
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
        // disable node sdk retries and timeout to prevent InvalidPart
        // and SocketHangUp errors. If retries are allowed, sdk will send
        // another request after first request has already deleted parts,
        // causing InvalidPart. Meanwhile, if request takes too long to finish,
        // sdk will create SocketHangUp error before response.
        // Custom request handler with no timeouts
        const requestHandler = new NodeHttpHandler({
            requestTimeout: 0,
            connectionTimeout: 0,
        });
        
        s3 = new S3Client({
            ...config,
            maxAttempts: 1,
            requestHandler,
        });
        
        s3.send(new CreateBucketCommand({ Bucket: bucket }))
            .then(() => done())
            .catch(err => done(err));
    });

    after(done => {
        s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: key }))
            .then(() => s3.send(new DeleteBucketCommand({ Bucket: bucket })))
            .then(() => done())
            .catch(err => {
                process.stdout.write(`err deleting object in after: ${err}\n`);
                return done(err);
            });
    });

    const itSkipIfAWS = process.env.AWS_ON_AIR ? it.skip : it;
    // will fail on AWS because parts too small

    itSkipIfAWS('should intiate, put parts and complete mpu ' +
        `with ${partCount} parts`, done => {
        process.stdout.write('***Running large MPU test***\n');
        let uploadId;     
        return waterfall([
            next => {
                s3.send(new CreateMultipartUploadCommand({ Bucket: bucket, Key: key }))
                    .then(data => {
                        process.stdout.write('initiated mpu\n');
                        uploadId = data.UploadId;
                        return next();
                    })
                    .catch(err => next(err));
            },
            next => {
                process.stdout.write('putting parts\n');
                return timesLimit(partCount, 20, (n, cb) =>
                    uploadPart(n, uploadId, s3, cb), err => {
                        if (err) {
                            process.stdout.write(`Error in timesLimit: ${err}\n`);
                        }
                        return next(err);
                    });
            },
            next => {
                const parts = [];
                for (let i = 0; i < partCount; i++) {
                    if (!partETags[i]) {
                        return next(new Error(`Missing ETag for part ${i + 1}`));
                    }
                    parts.push({
                        ETag: partETags[i],
                        PartNumber: i + 1,
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
                return s3.send(new CompleteMultipartUploadCommand(params))
                    .then(() => {
                        process.stdout.write('mpu completed successfully\n');
                        next();
                    })
                    .catch(err => next(err));
            },
            next => {
                process.stdout.write('about to get object\n');
                s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }))
                    .then(data => {
                        assert.strictEqual(data.ETag,
                                `"${finalETag}-${partCount}"`);
                        process.stdout.write('get object successful\n');
                        return next();
                    })
                    .catch(err => next(err));
            },
        ], done);
    });
});
