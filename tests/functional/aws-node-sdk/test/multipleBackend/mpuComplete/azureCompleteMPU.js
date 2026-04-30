const async = require('async');
const assert = require('assert');
const { CreateBucketCommand,
    CreateMultipartUploadCommand,
    UploadPartCommand,
    CompleteMultipartUploadCommand,
    PutObjectCommand,
    GetObjectCommand } = require('@aws-sdk/client-s3');
const { s3middleware } = require('arsenal');
const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const {
    fileLocation,
    awsS3,
    awsLocation,
    awsBucket,
    azureLocation,
    azureLocationMismatch,
    getAzureClient,
    getAzureContainerName,
    genUniqID,
    describeSkipIfNotMultiple,
} = require('../utils');

const azureMpuUtils = s3middleware.azureHelper.mpuUtils;

const azureContainerName = getAzureContainerName(azureLocation);
const azureClient = getAzureClient();
const azureTimeout = 20000;

const maxSubPartSize = azureMpuUtils.maxSubPartSize;
const smallBody = Buffer.from('I am a body', 'utf8');
const bigBody = Buffer.alloc(maxSubPartSize + 10);
const s3MD5 = 'bd43a393937412d119abcdbbc9bd363a-2';
const expectedContentLength = '104857621';

let s3;
let bucketUtil;

function getCheck(key, bucketMatch, cb) {
    let azureKey = key;
    s3.send(new GetObjectCommand({ Bucket: azureContainerName, Key: azureKey }))
        .then(s3Res => {
            assert.strictEqual(s3Res.ETag, `"${s3MD5}"`);

            if (!bucketMatch) {
                azureKey = `${azureContainerName}/${key}`;
            }
            return azureClient.getContainerClient(azureContainerName).getBlobClient(azureKey).getProperties();
        })
        .then(azureRes => {
            assert.strictEqual(expectedContentLength, azureRes.contentLength);
            cb();
        })
        .catch(err => {
            cb(err);
        });
}
function mpuSetup(key, location, cb) {
    const partArray = [];
    async.waterfall([
        next => {
            const params = {
                Bucket: azureContainerName,
                Key: key,
                Metadata: { 'scal-location-constraint': location },
            };
            s3.send(new CreateMultipartUploadCommand(params))
                .then(res => {
                    const uploadId = res.UploadId;
                    assert(uploadId);
                    assert.strictEqual(res.Bucket, azureContainerName);
                    assert.strictEqual(res.Key, key);
                    return next(null, uploadId);
                })
                .catch(next);
        },
        (uploadId, next) => {
            const partParams = {
                Bucket: azureContainerName,
                Key: key,
                PartNumber: 1,
                UploadId: uploadId,
                Body: smallBody,
            };
            s3.send(new UploadPartCommand(partParams))
                .then(res => {
                    partArray.push({ ETag: res.ETag, PartNumber: 1 });
                    return next(null, uploadId);
                })
                .catch(next);
        },
        (uploadId, next) => {
            const partParams = {
                Bucket: azureContainerName,
                Key: key,
                PartNumber: 2,
                UploadId: uploadId,
                Body: bigBody,
            };
            s3.send(new UploadPartCommand(partParams))
                .then(res => {
                    partArray.push({ ETag: res.ETag, PartNumber: 2 });
                    return next(null, uploadId);
                })
                .catch(next);
        },
    ], (err, uploadId) => {
        if (err) {
            return cb(err);
        }
        process.stdout.write('Created MPU and put two parts\n');
        return cb(uploadId, partArray);
    });
}

describeSkipIfNotMultiple('Complete MPU API for Azure data backend',
function testSuite() {
    this.timeout(150000);
    withV4(sigCfg => {
        beforeEach(function beFn() {
            this.currentTest.key = `somekey-${genUniqID()}`;
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            this.currentTest.awsClient = awsS3;
            return s3.send(new CreateBucketCommand({ Bucket: azureContainerName }))
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            });
        });

        afterEach(() => {
            process.stdout.write('Emptying bucket\n');
            return bucketUtil.empty(azureContainerName)
            .then(() => {
                process.stdout.write('Deleting bucket\n');
                return bucketUtil.deleteOne(azureContainerName);
            })
            .catch(err => {
                process.stdout.write(`Error in afterEach: ${err}\n`);
                throw err;
            });
        });

        it('should complete an MPU on Azure', function itFn(done) {
            mpuSetup(this.test.key, azureLocation, (uploadId, partArray) => {
                const params = {
                    Bucket: azureContainerName,
                    Key: this.test.key,
                    UploadId: uploadId,
                    MultipartUpload: { Parts: partArray },
                };
                s3.send(new CompleteMultipartUploadCommand(params))
                    .then(() => {
                        setTimeout(() => getCheck(this.test.key, true, done),
                            azureTimeout);
                    })
                    .catch(done);
            });
        });

        it('should complete an MPU on Azure with bucketMatch=false',
        function itFn(done) {
            mpuSetup(this.test.key, azureLocationMismatch,
            (uploadId, partArray) => {
                const params = {
                    Bucket: azureContainerName,
                    Key: this.test.key,
                    UploadId: uploadId,
                    MultipartUpload: { Parts: partArray },
                };
                s3.send(new CompleteMultipartUploadCommand(params))
                    .then(() => {
                        setTimeout(() => getCheck(this.test.key, false, done),
                            azureTimeout);
                    })
                    .catch(done);
            });
        });

        it('should complete an MPU on Azure with same key as object put ' +
        'to file', function itFn(done) {
            const body = Buffer.from('I am a body', 'utf8');
            s3.send(new PutObjectCommand({
                Bucket: azureContainerName,
                Key: this.test.key,
                Body: body,
                Metadata: { 'scal-location-constraint': fileLocation } })).then(() => {
                mpuSetup(this.test.key, azureLocation,
                (uploadId, partArray) => {
                    const params = {
                        Bucket: azureContainerName,
                        Key: this.test.key,
                        UploadId: uploadId,
                        MultipartUpload: { Parts: partArray },
                    };
                    s3.send(new CompleteMultipartUploadCommand(params))
                        .then(() => {
                            setTimeout(() => getCheck(this.test.key, true, done),
                                azureTimeout);
                        })
                        .catch(done);
                });
            }).catch(done);
        });

        it('should complete an MPU on Azure with same key as object put ' +
        'to Azure', function itFn(done) {
            const body = Buffer.from('I am a body', 'utf8');
            s3.send(new PutObjectCommand({
                Bucket: azureContainerName,
                Key: this.test.key,
                Body: body,
                Metadata: { 'scal-location-constraint': azureLocation } })).then(() => {
                mpuSetup(this.test.key, azureLocation,
                (uploadId, partArray) => {
                    const params = {
                        Bucket: azureContainerName,
                        Key: this.test.key,
                        UploadId: uploadId,
                        MultipartUpload: { Parts: partArray },
                    };
                    s3.send(new CompleteMultipartUploadCommand(params)).then(() => {

                        setTimeout(() => getCheck(this.test.key, true, done),
                            azureTimeout);
                    }).catch(err => {
                        assert.equal(err, null, `Err completing MPU: ${err}`);
                        done(err);
                    });
                });
            });
        });

        it('should complete an MPU on Azure with same key as object put ' +
        'to AWS', function itFn(done) {
            const body = Buffer.from('I am a body', 'utf8');
            s3.send(new PutObjectCommand({
                Bucket: azureContainerName,
                Key: this.test.key,
                Body: body,
                Metadata: { 'scal-location-constraint': awsLocation } 
            }))
                .then(() => {
                    mpuSetup(this.test.key, azureLocation,
                    (uploadId, partArray) => {
                        const params = {
                            Bucket: azureContainerName,
                            Key: this.test.key,
                            UploadId: uploadId,
                            MultipartUpload: { Parts: partArray },
                        };
                        s3.send(new CompleteMultipartUploadCommand(params))
                            .then(() => {
                                // make sure object is gone from AWS
                                setTimeout(() => {
                                    this.test.awsClient.send(new GetObjectCommand({ 
                                        Bucket: awsBucket,
                                        Key: this.test.key 
                                    }))
                                        .then(() => {
                                            done(new Error('Expected NoSuchKey error'));
                                        })
                                        .catch(err => {
                                            assert.strictEqual(err.name, 'NoSuchKey');
                                            getCheck(this.test.key, true, done);
                                        });
                                }, azureTimeout);
                            })
                            .catch(done);
                    });
                })
                .catch(done);
        });
    });
});
