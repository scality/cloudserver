const assert = require('assert');
const async = require('async');

const { s3middleware } = require('arsenal');
const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const { describeSkipIfNotMultipleOrCeph, expectedETag, uniqName, getAzureClient,
    getAzureContainerName, convertMD5, azureLocation, azureLocationMismatch }
    = require('../utils');
const azureMpuUtils = s3middleware.azureHelper.mpuUtils;
const maxSubPartSize = azureMpuUtils.maxSubPartSize;
const getBlockId = azureMpuUtils.getBlockId;

const keyObject = 'putazure';
const azureClient = getAzureClient();
const azureContainerName = getAzureContainerName(azureLocation);
const expectedMD5 = 'a63c90cc3684ad8b0a2176a6a8fe9005';

let bucketUtil;
let s3;

function checkSubPart(key, uploadId, expectedParts, cb) {
    azureClient.listBlocks(azureContainerName, key, 'all', (err, list) => {
        expect(err).toEqual(null);
        const uncommittedBlocks = list.UncommittedBlocks;
        const committedBlocks = list.CommittedBlocks;
        expect(committedBlocks).toBe(undefined);
        uncommittedBlocks.forEach((l, index) => {
            expect(l.Name).toBe(getBlockId(uploadId,
                expectedParts[index].partnbr, expectedParts[index].subpartnbr));
            expect(l.Size).toBe(expectedParts[index].size.toString());
        });
        cb();
    });
}

function azureCheck(key, cb) {
    s3.getObject({ Bucket: azureContainerName, Key: key }, (err, res) => {
        expect(err).toEqual(null);
        expect(res.ETag).toBe(`"${expectedMD5}"`);
        azureClient.getBlobProperties(azureContainerName, key, (err, res) => {
            const convertedMD5 = convertMD5(res.contentSettings.contentMD5);
            expect(convertedMD5).toBe(expectedMD5);
            return cb();
        });
    });
}

describeSkipIfNotMultipleOrCeph('MultipleBackend put part to AZURE', function
describeF() {
    this.timeout(80000);
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
                    next => s3.createBucket({ Bucket: azureContainerName,
                    }, err => next(err)),
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

            afterEach(done => {
                async.waterfall([
                    next => s3.abortMultipartUpload({
                        Bucket: azureContainerName,
                        Key: testContext.currentTest.key,
                        UploadId: testContext.currentTest.uploadId,
                    }, err => next(err)),
                    next => s3.deleteBucket({ Bucket: azureContainerName },
                      err => next(err)),
                ], err => {
                    expect(err).toEqual(null);
                    done();
                });
            });

            test('should put 0-byte block to Azure', done => {
                const params = {
                    Bucket: azureContainerName,
                    Key: testContext.test.key,
                    UploadId: testContext.test.uploadId,
                    PartNumber: 1,
                };
                async.waterfall([
                    next => s3.uploadPart(params, (err, res) => {
                        const eTagExpected = `"${azureMpuUtils.zeroByteETag}"`;
                        expect(res.ETag).toBe(eTagExpected);
                        return next(err);
                    }),
                    next => azureClient.listBlocks(azureContainerName,
                    testContext.test.key, 'all', err => {
                        expect(err).not.toEqual(null);
                        expect(err.code).toBe('BlobNotFound');
                        next();
                    }),
                ], done);
            });

            test('should put 2 blocks to Azure', done => {
                const body = Buffer.alloc(maxSubPartSize + 10);
                const parts = [{ partnbr: 1, subpartnbr: 0,
                    size: maxSubPartSize },
                  { partnbr: 1, subpartnbr: 1, size: 10 }];
                const params = {
                    Bucket: azureContainerName,
                    Key: testContext.test.key,
                    UploadId: testContext.test.uploadId,
                    PartNumber: 1,
                    Body: body,
                };
                async.waterfall([
                    next => s3.uploadPart(params, (err, res) => {
                        const eTagExpected = expectedETag(body);
                        expect(res.ETag).toBe(eTagExpected);
                        return next(err);
                    }),
                    next => checkSubPart(testContext.test.key, testContext.test.uploadId,
                    parts, next),
                ], done);
            });

            test(
                'should put 5 parts bigger than maxSubPartSize to Azure',
                done => {
                    const body = Buffer.alloc(maxSubPartSize + 10);
                    let parts = [];
                    for (let i = 1; i < 6; i++) {
                        parts = parts.concat([
                          { partnbr: i, subpartnbr: 0, size: maxSubPartSize },
                          { partnbr: i, subpartnbr: 1, size: 10 },
                        ]);
                    }
                    async.times(5, (n, next) => {
                        const partNumber = n + 1;
                        const params = {
                            Bucket: azureContainerName,
                            Key: testContext.test.key,
                            UploadId: testContext.test.uploadId,
                            PartNumber: partNumber,
                            Body: body,
                        };
                        s3.uploadPart(params, (err, res) => {
                            const eTagExpected = expectedETag(body);
                            expect(res.ETag).toBe(eTagExpected);
                            return next(err);
                        });
                    }, err => {
                        expect(err).toEqual(null);
                        checkSubPart(testContext.test.key, testContext.test.uploadId,
                        parts, done);
                    });
                }
            );

            test(
                'should put 5 parts smaller than maxSubPartSize to Azure',
                done => {
                    const body = Buffer.alloc(10);
                    let parts = [];
                    for (let i = 1; i < 6; i++) {
                        parts = parts.concat([
                          { partnbr: i, subpartnbr: 0, size: 10 },
                        ]);
                    }
                    async.times(5, (n, next) => {
                        const partNumber = n + 1;
                        const params = {
                            Bucket: azureContainerName,
                            Key: testContext.test.key,
                            UploadId: testContext.test.uploadId,
                            PartNumber: partNumber,
                            Body: body,
                        };
                        s3.uploadPart(params, (err, res) => {
                            const eTagExpected = expectedETag(body);
                            expect(res.ETag).toBe(eTagExpected);
                            return next(err);
                        });
                    }, err => {
                        expect(err).toEqual(null);
                        checkSubPart(testContext.test.key, testContext.test.uploadId,
                        parts, done);
                    });
                }
            );

            test('should put the same part twice', done => {
                const body1 = Buffer.alloc(maxSubPartSize + 10);
                const body2 = Buffer.alloc(20);
                const parts2 = [{ partnbr: 1, subpartnbr: 0, size: 20 },
                  { partnbr: 1, subpartnbr: 1, size: 10 }];
                async.waterfall([
                    next => s3.uploadPart({
                        Bucket: azureContainerName,
                        Key: testContext.test.key,
                        UploadId: testContext.test.uploadId,
                        PartNumber: 1,
                        Body: body1,
                    }, err => next(err)),
                    next => s3.uploadPart({
                        Bucket: azureContainerName,
                        Key: testContext.test.key,
                        UploadId: testContext.test.uploadId,
                        PartNumber: 1,
                        Body: body2,
                    }, (err, res) => {
                        const eTagExpected = expectedETag(body2);
                        expect(res.ETag).toBe(eTagExpected);
                        return next(err);
                    }),
                    next => checkSubPart(testContext.test.key, testContext.test.uploadId,
                    parts2, next),
                ], done);
            });
        });

        describe('with same key as preexisting part', () => {
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

            afterEach(done => {
                async.waterfall([
                    next => {
                        process.stdout.write('Aborting multipart upload\n');
                        s3.abortMultipartUpload({
                            Bucket: azureContainerName,
                            Key: testContext.currentTest.key,
                            UploadId: testContext.currentTest.uploadId },
                        err => next(err));
                    },
                    next => {
                        process.stdout.write('Deleting object\n');
                        s3.deleteObject({
                            Bucket: azureContainerName,
                            Key: testContext.currentTest.key },
                        err => next(err));
                    },
                    next => {
                        process.stdout.write('Deleting bucket\n');
                        s3.deleteBucket({
                            Bucket: azureContainerName },
                        err => next(err));
                    },
                ], err => {
                    expect(err).toEqual(null);
                    done();
                });
            });

            test(
                'should put a part without overwriting existing object',
                done => {
                    const body = Buffer.alloc(20);
                    s3.uploadPart({
                        Bucket: azureContainerName,
                        Key: testContext.test.key,
                        UploadId: testContext.test.uploadId,
                        PartNumber: 1,
                        Body: body,
                    }, err => {
                        expect(err).toBe(null);
                        azureCheck(testContext.test.key, done);
                    });
                }
            );
        });
    });
});

describeSkipIfNotMultipleOrCeph('MultipleBackend put part to AZURE ' +
'location with bucketMatch sets to false', function
describeF() {
    this.timeout(80000);
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
                    next => s3.createBucket({ Bucket: azureContainerName,
                    }, err => next(err)),
                    next => s3.createMultipartUpload({
                        Bucket: azureContainerName,
                        Key: testContext.currentTest.key,
                        Metadata: { 'scal-location-constraint':
                        azureLocationMismatch },
                    }, (err, res) => {
                        if (err) {
                            return next(err);
                        }
                        testContext.currentTest.uploadId = res.UploadId;
                        return next();
                    }),
                ], done);
            });

            afterEach(done => {
                async.waterfall([
                    next => s3.abortMultipartUpload({
                        Bucket: azureContainerName,
                        Key: testContext.currentTest.key,
                        UploadId: testContext.currentTest.uploadId,
                    }, err => next(err)),
                    next => s3.deleteBucket({ Bucket: azureContainerName },
                      err => next(err)),
                ], err => {
                    expect(err).toEqual(null);
                    done();
                });
            });

            test('should put block to AZURE location with bucketMatch' +
            ' sets to false', done => {
                const body20 = Buffer.alloc(20);
                const params = {
                    Bucket: azureContainerName,
                    Key: testContext.test.key,
                    UploadId: testContext.test.uploadId,
                    PartNumber: 1,
                    Body: body20,
                };
                const parts = [{ partnbr: 1, subpartnbr: 0,
                    size: 20 }];
                async.waterfall([
                    next => s3.uploadPart(params, (err, res) => {
                        const eTagExpected =
                        '"441018525208457705bf09a8ee3c1093"';
                        expect(res.ETag).toBe(eTagExpected);
                        return next(err);
                    }),
                    next => checkSubPart(
                      `${azureContainerName}/${testContext.test.key}`,
                      testContext.test.uploadId, parts, next),
                ], done);
            });
        });
    });
});
