const async = require('async');
const assert = require('assert');

const { s3middleware } = require('arsenal');
const { config } = require('../../../../../../lib/Config');
const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const { getAzureClient, getAzureContainerName } =
    require('../utils');

const azureMpuUtils = s3middleware.azureHelper.mpuUtils;
const describeSkipIfNotMultiple = (config.backends.data !== 'multiple'
    || process.env.S3_END_TO_END) ? describe.skip : describe;

const awsLocation = 'aws-test';
const awsBucket = 'multitester555';
const azureLocation = 'azuretest';
const azureLocationMismatch = 'azuretestmismatch';
const azureContainerName = getAzureContainerName();
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
    s3.getObject({ Bucket: azureContainerName, Key: azureKey },
    (err, s3Res) => {
        assert.equal(err, null, `Err getting object from S3: ${err}`);
        assert.strictEqual(s3Res.ETag, `"${s3MD5}"`);

        if (!bucketMatch) {
            azureKey = `${azureContainerName}/${key}`;
        }
        azureClient.getBlobProperties(azureContainerName, azureKey,
        (err, azureRes) => {
            assert.equal(err, null, `Err getting object from Azure: ${err}`);
            assert.strictEqual(expectedContentLength, azureRes.contentLength);
            cb();
        });
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
            s3.createMultipartUpload(params, (err, res) => {
                const uploadId = res.UploadId;
                assert(uploadId);
                assert.strictEqual(res.Bucket, azureContainerName);
                assert.strictEqual(res.Key, key);
                next(err, uploadId);
            });
        },
        (uploadId, next) => {
            const partParams = {
                Bucket: azureContainerName,
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
                Bucket: azureContainerName,
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

describeSkipIfNotMultiple('Complete MPU API for Azure data backend',
function testSuite() {
    this.timeout(100000);
    withV4(sigCfg => {
        beforeEach(function beFn() {
            this.currentTest.key = `somekey-${Date.now()}`;
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            const awsConfig = getRealAwsConfig(awsLocation);
            this.currentTest.awsClient = new AWS.S3(awsConfig);
            return s3.createBucketAsync({ Bucket: azureContainerName })
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
                s3.completeMultipartUpload(params, err => {
                    assert.equal(err, null, `Err completing MPU: ${err}`);
                    setTimeout(() => getCheck(this.test.key, true, done),
                        azureTimeout);
                });
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
                s3.completeMultipartUpload(params, err => {
                    assert.equal(err, null, `Err completing MPU: ${err}`);
                    setTimeout(() => getCheck(this.test.key, false, done),
                        azureTimeout);
                });
            });
        });

        it('should complete an MPU on Azure with same key as object put ' +
        'to file', function itFn(done) {
            const body = Buffer.from('I am a body', 'utf8');
            s3.putObject({
                Bucket: azureContainerName,
                Key: this.test.key,
                Body: body,
                Metadata: { 'scal-location-constraint': 'file' } },
            err => {
                assert.equal(err, null, `Err putting object to file: ${err}`);
                mpuSetup(this.test.key, azureLocation,
                (uploadId, partArray) => {
                    const params = {
                        Bucket: azureContainerName,
                        Key: this.test.key,
                        UploadId: uploadId,
                        MultipartUpload: { Parts: partArray },
                    };
                    s3.completeMultipartUpload(params, err => {
                        assert.equal(err, null, `Err completing MPU: ${err}`);
                        setTimeout(() => getCheck(this.test.key, true, done),
                            azureTimeout);
                    });
                });
            });
        });

        it('should complete an MPU on Azure with same key as object put ' +
        'to Azure', function itFn(done) {
            const body = Buffer.from('I am a body', 'utf8');
            s3.putObject({
                Bucket: azureContainerName,
                Key: this.test.key,
                Body: body,
                Metadata: { 'scal-location-constraint': azureLocation } },
            err => {
                assert.equal(err, null, `Err putting object to Azure: ${err}`);
                mpuSetup(this.test.key, azureLocation,
                (uploadId, partArray) => {
                    const params = {
                        Bucket: azureContainerName,
                        Key: this.test.key,
                        UploadId: uploadId,
                        MultipartUpload: { Parts: partArray },
                    };
                    s3.completeMultipartUpload(params, err => {
                        assert.equal(err, null, `Err completing MPU: ${err}`);
                        setTimeout(() => getCheck(this.test.key, true, done),
                            azureTimeout);
                    });
                });
            });
        });

        it('should complete an MPU on Azure with same key as object put ' +
        'to AWS', function itFn(done) {
            const body = Buffer.from('I am a body', 'utf8');
            s3.putObject({
                Bucket: azureContainerName,
                Key: this.test.key,
                Body: body,
                Metadata: { 'scal-location-constraint': awsLocation } },
            err => {
                assert.equal(err, null, `Err putting object to AWS: ${err}`);
                mpuSetup(this.test.key, azureLocation,
                (uploadId, partArray) => {
                    const params = {
                        Bucket: azureContainerName,
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
                        }, azureTimeout);
                    });
                });
            });
        });
    });
});
