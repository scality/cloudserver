const assert = require('assert');
const async = require('async');

const { s3middleware } = require('arsenal');
const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const { describeSkipIfNotMultipleOrCeph, uniqName, getAzureClient,
    getAzureContainerName, convertMD5, azureLocation } = require('../utils');
const azureMpuUtils = s3middleware.azureHelper.mpuUtils;
const maxSubPartSize = azureMpuUtils.maxSubPartSize;

const keyObject = 'abortazure';
const azureClient = getAzureClient();
const azureContainerName = getAzureContainerName(azureLocation);
const expectedMD5 = 'a63c90cc3684ad8b0a2176a6a8fe9005';

let bucketUtil;
let s3;

function azureCheck(container, key, expected, cb) {
    azureClient.getBlobProperties(container, key, (err, res) => {
        if (expected.error) {
            expect(err.statusCode).toBe(404);
            expect(err.code).toBe('NotFound');
        } else {
            const convertedMD5 = convertMD5(res.contentSettings.contentMD5);
            expect(convertedMD5).toBe(expectedMD5);
        }
        return cb();
    });
}

describeSkipIfNotMultipleOrCeph('Abort MPU on Azure data backend', function
describeF() {
    this.timeout(50000);
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
                    next => s3.createBucket({ Bucket: azureContainerName },
                        err => next(err)),
                    next => s3.createMultipartUpload({
                        Bucket: azureContainerName,
                        Key: testContext.currentTest.key,
                        Metadata: { 'scal-location-constraint': azureLocation },
                    }, (err, res) => {
                        if (err) {
                            return next(err);
                        }
                        testContext.currentTest.uploadId = res.UploadId;
                        return next();
                    }),
                ], done);
            });

            afterEach(done => s3.deleteBucket({ Bucket: azureContainerName },
                done));

            test('should abort an MPU with one empty part ', done => {
                const expected = { error: true };
                const params = {
                    Bucket: azureContainerName,
                    Key: testContext.test.key,
                    UploadId: testContext.test.uploadId,
                };
                async.waterfall([
                    next => {
                        const partParams = Object.assign({ PartNumber: 1 },
                            params);
                        s3.uploadPart(partParams, err => {
                            expect(err).toBe(null);
                            return next();
                        });
                    },
                    next => s3.abortMultipartUpload(params, err => next(err)),
                    next => azureCheck(azureContainerName, testContext.test.key,
                    expected, next),
                ], done);
            });

            test(
                'should abort MPU with one part bigger than max subpart',
                done => {
                    const expected = { error: true };
                    const params = {
                        Bucket: azureContainerName,
                        Key: testContext.test.key,
                        UploadId: testContext.test.uploadId,
                    };
                    async.waterfall([
                        next => {
                            const body = Buffer.alloc(maxSubPartSize + 10);
                            const partParams = Object.assign(
                                { PartNumber: 1, Body: body }, params);
                            s3.uploadPart(partParams, err => {
                                expect(err).toBe(null);
                                return next();
                            });
                        },
                        next => s3.abortMultipartUpload(params, err => next(err)),
                        next => azureCheck(azureContainerName, testContext.test.key,
                        expected, next),
                    ], done);
                }
            );
        });

        describe('with previously existing object with same key', () => {
            let testContext;

            beforeEach(() => {
                testContext = {};
            });

            beforeEach(done => {
                async.waterfall([
                    next => s3.createBucket({ Bucket: azureContainerName },
                        err => next(err)),
                    next => {
                        const body = Buffer.alloc(10);
                        s3.putObject({
                            Bucket: azureContainerName,
                            Key: testContext.currentTest.key,
                            Metadata: { 'scal-location-constraint':
                                azureLocation },
                            Body: body,
                        }, err => {
                            expect(err).toEqual(null);
                            return next();
                        });
                    },
                    next => s3.createMultipartUpload({
                        Bucket: azureContainerName,
                        Key: testContext.currentTest.key,
                        Metadata: { 'scal-location-constraint': azureLocation },
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

            test('should abort MPU without deleting existing object', done => {
                const expected = { error: false };
                const params = {
                    Bucket: azureContainerName,
                    Key: testContext.test.key,
                    UploadId: testContext.test.uploadId,
                };
                async.waterfall([
                    next => {
                        const body = Buffer.alloc(10);
                        const partParams = Object.assign(
                            { PartNumber: 1, Body: body }, params);
                        s3.uploadPart(partParams, err => {
                            expect(err).toBe(null);
                            return next();
                        });
                    },
                    next => s3.abortMultipartUpload(params, err => next(err)),
                    next => azureCheck(azureContainerName, testContext.test.key,
                    expected, next),
                ], done);
            });
        });
    });
});
