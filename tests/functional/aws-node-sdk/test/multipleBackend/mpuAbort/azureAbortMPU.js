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

const { s3middleware } = require('arsenal');
const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const { uniqName, getAzureClient,
    getAzureContainerName, convertMD5, azureLocation,
    describeSkipIfNotMultiple } = require('../utils');
const azureMpuUtils = s3middleware.azureHelper.mpuUtils;
const maxSubPartSize = azureMpuUtils.maxSubPartSize;

const keyObject = 'abortazure';
const azureClient = getAzureClient();
const azureContainerName = getAzureContainerName(azureLocation);
const expectedMD5 = 'a63c90cc3684ad8b0a2176a6a8fe9005';

let bucketUtil;
let s3;

function azureCheck(container, key, expected, cb) {
    azureClient.getContainerClient(container).getProperties(key).then(res => {
        assert.ok(!expected.error);
        const convertedMD5 = convertMD5(res.contentSettings.contentMD5);
        assert.strictEqual(convertedMD5, expectedMD5);
        return cb();
    },
    err => {
        assert.ok(expected.error);
        assert.strictEqual(err.statusCode, 404);
        assert.strictEqual(err.code, 'NotFound');
        return cb();
    });
}

describeSkipIfNotMultiple('Abort MPU on Azure data backend', function
describeF() {
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
                    next => {
                        s3.send(new CreateBucketCommand({
                            Bucket: azureContainerName,
                        }))
                            .then(() => next())
                            .catch(next);
                    },
                    next => {
                        s3.send(new CreateMultipartUploadCommand({
                            Bucket: azureContainerName,
                            Key: this.currentTest.key,
                            Metadata: {
                                'scal-location-constraint': azureLocation,
                            },
                        }))
                            .then(res => {
                                this.currentTest.uploadId = res.UploadId;
                                next();
                            })
                            .catch(next);
                    },
                ], done);
            });

            afterEach(done => {
                s3.send(new DeleteBucketCommand({
                    Bucket: azureContainerName,
                }))
                    .then(() => done())
                    .catch(done);
            });

            it('should abort an MPU with one empty part ', function itFn(done) {
                const expected = { error: true };
                const params = {
                    Bucket: azureContainerName,
                    Key: this.test.key,
                    UploadId: this.test.uploadId,
                };
                async.waterfall([
                    next => {
                        const partParams = {
                            ...params,
                            PartNumber: 1,
                            Body: Buffer.alloc(0),
                        };
                        s3.send(new UploadPartCommand(partParams))
                            .then(() => next())
                            .catch(next);
                    },
                    next => {
                        s3.send(new AbortMultipartUploadCommand(params))
                            .then(() => next())
                            .catch(next);
                    },
                    next => azureCheck(azureContainerName, this.test.key,
                    expected, next),
                ], done);
            });

            it('should abort MPU with one part bigger than max subpart',
            function itFn(done) {
                const expected = { error: true };
                const params = {
                    Bucket: azureContainerName,
                    Key: this.test.key,
                    UploadId: this.test.uploadId,
                };
                async.waterfall([
                    next => {
                        const body = Buffer.alloc(maxSubPartSize + 10);
                        const partParams = {
                            ...params,
                            PartNumber: 1,
                            Body: body,
                        };
                        s3.send(new UploadPartCommand(partParams))
                            .then(() => next())
                            .catch(next);
                    },
                    next => {
                        s3.send(new AbortMultipartUploadCommand(params))
                            .then(() => next())
                            .catch(next);
                    },
                    next => azureCheck(azureContainerName, this.test.key,
                    expected, next),
                ], done);
            });
        });

        describe('with previously existing object with same key', () => {
            beforeEach(function beforeEachFn(done) {
                async.waterfall([
                    next => {
                        s3.send(new CreateBucketCommand({
                            Bucket: azureContainerName,
                        }))
                            .then(() => next())
                            .catch(next);
                    },
                    next => {
                        const body = Buffer.alloc(10);
                        s3.send(new PutObjectCommand({
                            Bucket: azureContainerName,
                            Key: this.currentTest.key,
                            Metadata: {
                                'scal-location-constraint': azureLocation,
                            },
                            Body: body,
                        }))
                            .then(() => next())
                            .catch(err => {
                                assert.equal(err, null, 'Err putting object to ' +
                                `azure: ${err}`);
                                next(err);
                            });
                    },
                    next => {
                        s3.send(new CreateMultipartUploadCommand({
                            Bucket: azureContainerName,
                            Key: this.currentTest.key,
                            Metadata: {
                                'scal-location-constraint': azureLocation,
                            },
                        }))
                            .then(res => {
                                this.currentTest.uploadId = res.UploadId;
                                next();
                            })
                            .catch(next);
                    },
                ], done);
            });

            afterEach(() => {
                process.stdout.write('Emptying bucket\n');
                return bucketUtil.empty(azureContainerName)
                .then(() => {
                    process.stdout.write('Deleting bucket\n');
                    return bucketUtil.deleteOne(azureContainerName);
                })
                .catch(err => {
                    process.stdout.write('Error emptying/deleting bucket: ' +
                    `${err}\n`);
                    throw err;
                });
            });

            it('should abort MPU without deleting existing object',
            function itFn(done) {
                const expected = { error: false };
                const params = {
                    Bucket: azureContainerName,
                    Key: this.test.key,
                    UploadId: this.test.uploadId,
                };
                async.waterfall([
                    next => {
                        const body = Buffer.alloc(10);
                        const partParams = {
                            ...params,
                            PartNumber: 1,
                            Body: body,
                        };
                        s3.send(new UploadPartCommand(partParams))
                            .then(() => next())
                            .catch(next);
                    },
                    next => {
                        s3.send(new AbortMultipartUploadCommand(params))
                            .then(() => next())
                            .catch(next);
                    },
                    next => azureCheck(azureContainerName, this.test.key,
                    expected, next),
                ], done);
            });
        });
    });
});
