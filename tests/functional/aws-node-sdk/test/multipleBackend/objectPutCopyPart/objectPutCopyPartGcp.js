const async = require('async');
const assert = require('assert');

const { config } = require('../../../../../../lib/Config');
const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const { describeSkipIfNotMultipleOrCeph, uniqName, gcpBucketMPU,
    gcpClient, gcpLocation, gcpLocationMismatch, memLocation,
    awsLocation, awsS3, getOwnerInfo, genUniqID } = require('../utils');

const bucket = `partcopygcp${genUniqID()}`;

const memBucketName = `memeputcopypartgcp${genUniqID()}`;
const awsBucketName = `awsputcopypartgcp${genUniqID()}`;

const normalBodySize = 11;
const normalBody = Buffer.from('I am a body', 'utf8');
const normalMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';

const sixBytesMD5 = 'c978a461602f0372b5f970157927f723';

const oneKb = 1024;
const oneKbBody = Buffer.alloc(oneKb);
const oneKbMD5 = '0f343b0931126a20f133d67c2b018a3b';

const fiveMB = 5 * 1024 * 1024;
const fiveMbBody = Buffer.alloc(fiveMB);
const fiveMbMD5 = '5f363e0e58a95f06cbe9bbc662c5dfb6';

const keyObjectGcp = 'objectputcopypartgcp';
const keyObjectMemory = 'objectputcopypartMemory';
const keyObjectAWS = 'objectputcopypartAWS';

const { ownerID, ownerDisplayName } = getOwnerInfo('account1');

const result = {
    Bucket: '',
    Key: '',
    UploadId: '',
    MaxParts: 1000,
    IsTruncated: false,
    Parts: [],
    Initiator:
     { ID: ownerID,
       DisplayName: ownerDisplayName },
    Owner:
     { DisplayName: ownerDisplayName,
       ID: ownerID },
    StorageClass: 'STANDARD',
};

let s3;
let bucketUtil;

function assertCopyPart(infos, cb) {
    const { bucketName, keyName, uploadId, md5, totalSize } = infos;
    const resultCopy = JSON.parse(JSON.stringify(result));
    resultCopy.Bucket = bucketName;
    resultCopy.Key = keyName;
    resultCopy.UploadId = uploadId;
    async.waterfall([
        next => s3.listParts({
            Bucket: bucketName,
            Key: keyName,
            UploadId: uploadId,
        }, (err, res) => {
            assert.ifError(err, 'listParts: Expected success,' +
            ` got error: ${err}`);
            resultCopy.Parts =
             [{ PartNumber: 1,
                 LastModified: res.Parts[0].LastModified,
                 ETag: `"${md5}"`,
                 Size: totalSize }];
            assert.deepStrictEqual(res, resultCopy);
            next();
        }),
        next => gcpClient.listParts({
            Bucket: gcpBucketMPU,
            Key: keyName,
            UploadId: uploadId,
        }, (err, res) => {
            assert.ifError(err, 'GCP listParts: Expected success,' +
                `got error: ${err}`);
            expect(res.Contents[0].ETag).toBe(`"${md5}"`);
            next();
        }),
    ], cb);
}

describeSkipIfNotMultipleOrCeph('Put Copy Part to GCP', function describeFn() {
    this.timeout(800000);
    withV4(sigCfg => {
        beforeEach(done => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            s3.createBucket({ Bucket: bucket,
                CreateBucketConfiguration: {
                    LocationConstraint: gcpLocation,
                },
            }, done);
        });

        afterEach(() => {
            process.stdout.write('Emptying bucket\n');
            return bucketUtil.empty(bucket)
            .then(() => bucketUtil.empty(memBucketName))
            .then(() => {
                process.stdout.write(`Deleting bucket ${bucket}\n`);
                return bucketUtil.deleteOne(bucket);
            })
            .then(() => {
                process.stdout.write(`Deleting bucket ${memBucketName}\n`);
                return bucketUtil.deleteOne(memBucketName);
            })
            .catch(err => {
                process.stdout.write(`Error in afterEach: ${err}\n`);
                throw err;
            });
        });

        describe('Basic test: ', () => {
            let testContext;

            beforeEach(() => {
                testContext = {};
            });

            beforeEach(done => {
                testContext.currentTest.keyNameNormalGcp =
                    `normalgcp${uniqName(keyObjectGcp)}`;
                testContext.currentTest.keyNameNormalGcpMismatch =
                    `normalgcpmismatch${uniqName(keyObjectGcp)}`;

                testContext.currentTest.keyNameFiveMbGcp =
                    `fivembgcp${uniqName(keyObjectGcp)}`;
                testContext.currentTest.keyNameFiveMbMem =
                    `fivembmem${uniqName(keyObjectMemory)}`;

                testContext.currentTest.mpuKeyNameGcp =
                    `mpukeyname${uniqName(keyObjectGcp)}`;
                testContext.currentTest.mpuKeyNameMem =
                    `mpukeyname${uniqName(keyObjectMemory)}`;
                testContext.currentTest.mpuKeyNameAWS =
                    `mpukeyname${uniqName(keyObjectAWS)}`;
                const paramsGcp = {
                    Bucket: bucket,
                    Key: testContext.currentTest.mpuKeyNameGcp,
                    Metadata: { 'scal-location-constraint': gcpLocation },
                };
                const paramsMem = {
                    Bucket: memBucketName,
                    Key: testContext.currentTest.mpuKeyNameMem,
                    Metadata: { 'scal-location-constraint': memLocation },
                };
                const paramsAWS = {
                    Bucket: memBucketName,
                    Key: testContext.currentTest.mpuKeyNameAWS,
                    Metadata: { 'scal-location-constraint': awsLocation },
                };
                async.waterfall([
                    next => s3.createBucket({ Bucket: bucket },
                      err => next(err)),
                    next => s3.createBucket({ Bucket: memBucketName },
                      err => next(err)),
                    next => s3.putObject({
                        Bucket: bucket,
                        Key: testContext.currentTest.keyNameNormalGcp,
                        Body: normalBody,
                        Metadata: { 'scal-location-constraint': gcpLocation },
                    }, err => next(err)),
                    next => s3.putObject({
                        Bucket: bucket,
                        Key: testContext.currentTest.keyNameNormalGcpMismatch,
                        Body: normalBody,
                        Metadata: { 'scal-location-constraint':
                        gcpLocationMismatch },
                    }, err => next(err)),
                    next => s3.putObject({
                        Bucket: bucket,
                        Key: testContext.currentTest.keyNameFiveMbGcp,
                        Body: fiveMbBody,
                        Metadata: { 'scal-location-constraint': gcpLocation },
                    }, err => next(err)),
                    next => s3.putObject({
                        Bucket: bucket,
                        Key: testContext.currentTest.keyNameFiveMbMem,
                        Body: fiveMbBody,
                        Metadata: { 'scal-location-constraint': memLocation },
                    }, err => next(err)),
                    next => s3.createMultipartUpload(paramsGcp,
                    (err, res) => {
                        assert.ifError(err, 'createMultipartUpload ' +
                        `on gcp: Expected success, got error: ${err}`);
                        testContext.currentTest.uploadId = res.UploadId;
                        next();
                    }),
                    next => s3.createMultipartUpload(paramsMem,
                    (err, res) => {
                        assert.ifError(err, 'createMultipartUpload ' +
                        `in memory: Expected success, got error: ${err}`);
                        testContext.currentTest.uploadIdMem = res.UploadId;
                        next();
                    }),
                    next => s3.createMultipartUpload(paramsAWS,
                    (err, res) => {
                        assert.ifError(err, 'createMultipartUpload ' +
                        `on AWS: Expected success, got error: ${err}`);
                        testContext.currentTest.uploadIdAWS = res.UploadId;
                        next();
                    }),
                ], done);
            });

            afterEach(done => {
                const paramsGcp = {
                    Bucket: bucket,
                    Key: testContext.currentTest.mpuKeyNameGcp,
                    UploadId: testContext.currentTest.uploadId,
                };
                const paramsMem = {
                    Bucket: memBucketName,
                    Key: testContext.currentTest.mpuKeyNameMem,
                    UploadId: testContext.currentTest.uploadIdMem,
                };
                const paramsAWS = {
                    Bucket: memBucketName,
                    Key: testContext.currentTest.mpuKeyNameAWS,
                    UploadId: testContext.currentTest.uploadIdAWS,
                };
                async.waterfall([
                    next => s3.abortMultipartUpload(paramsGcp,
                      err => next(err)),
                    next => s3.abortMultipartUpload(paramsMem,
                      err => next(err)),
                    next => s3.abortMultipartUpload(paramsAWS,
                      err => next(err)),
                ], done);
            });

            test(
                'should copy small part from GCP to MPU with GCP location',
                done => {
                    const params = {
                        Bucket: bucket,
                        CopySource:
                          `${bucket}/${testContext.test.keyNameNormalGcp}`,
                        Key: testContext.test.mpuKeyNameGcp,
                        PartNumber: 1,
                        UploadId: testContext.test.uploadId,
                    };
                    async.waterfall([
                        next => s3.uploadPartCopy(params, (err, res) => {
                            assert.ifError(err, 'uploadPartCopy: Expected ' +
                            `success, got error: ${err}`);
                            expect(res.ETag).toBe(`"${normalMD5}"`);
                            next(err);
                        }),
                        next => {
                            const infos = {
                                bucketName: bucket,
                                keyName: testContext.test.mpuKeyNameGcp,
                                uploadId: testContext.test.uploadId,
                                md5: normalMD5,
                                totalSize: normalBodySize,
                            };
                            assertCopyPart(infos, next);
                        },
                    ], done);
                }
            );

            test('should copy small part from GCP with bucketMatch=false to ' +
            'MPU with GCP location', done => {
                const params = {
                    Bucket: bucket,
                    CopySource:
                      `${bucket}/${testContext.test.keyNameNormalGcpMismatch}`,
                    Key: testContext.test.mpuKeyNameGcp,
                    PartNumber: 1,
                    UploadId: testContext.test.uploadId,
                };
                async.waterfall([
                    next => s3.uploadPartCopy(params, (err, res) => {
                        assert.ifError(err, 'uploadPartCopy: Expected ' +
                        `success, got error: ${err}`);
                        expect(res.ETag).toBe(`"${normalMD5}"`);
                        next(err);
                    }),
                    next => {
                        const infos = {
                            bucketName: bucket,
                            keyName: testContext.test.mpuKeyNameGcp,
                            uploadId: testContext.test.uploadId,
                            md5: normalMD5,
                            totalSize: normalBodySize,
                        };
                        assertCopyPart(infos, next);
                    },
                ], done);
            });

            test(
                'should copy 5 Mb part from GCP to MPU with GCP location',
                done => {
                    const params = {
                        Bucket: bucket,
                        CopySource:
                          `${bucket}/${testContext.test.keyNameFiveMbGcp}`,
                        Key: testContext.test.mpuKeyNameGcp,
                        PartNumber: 1,
                        UploadId: testContext.test.uploadId,
                    };
                    async.waterfall([
                        next => s3.uploadPartCopy(params, (err, res) => {
                            assert.ifError(err, 'uploadPartCopy: Expected ' +
                            `success, got error: ${err}`);
                            expect(res.ETag).toBe(`"${fiveMbMD5}"`);
                            next(err);
                        }),
                        next => {
                            const infos = {
                                bucketName: bucket,
                                keyName: testContext.test.mpuKeyNameGcp,
                                uploadId: testContext.test.uploadId,
                                md5: fiveMbMD5,
                                totalSize: fiveMB,
                            };
                            assertCopyPart(infos, next);
                        },
                    ], done);
                }
            );

            test(
                'should copy part from GCP to MPU with memory location',
                done => {
                    const params = {
                        Bucket: memBucketName,
                        CopySource:
                          `${bucket}/${testContext.test.keyNameNormalGcp}`,
                        Key: testContext.test.mpuKeyNameMem,
                        PartNumber: 1,
                        UploadId: testContext.test.uploadIdMem,
                    };
                    async.waterfall([
                        next => s3.uploadPartCopy(params, (err, res) => {
                            assert.ifError(err, 'uploadPartCopy: Expected ' +
                            `success, got error: ${err}`);
                            expect(res.ETag).toBe(`"${normalMD5}"`);
                            next(err);
                        }),
                        next => {
                            s3.listParts({
                                Bucket: memBucketName,
                                Key: testContext.test.mpuKeyNameMem,
                                UploadId: testContext.test.uploadIdMem,
                            }, (err, res) => {
                                assert.ifError(err,
                                'listParts: Expected success,' +
                                ` got error: ${err}`);
                                const resultCopy =
                                JSON.parse(JSON.stringify(result));
                                resultCopy.Bucket = memBucketName;
                                resultCopy.Key = testContext.test.mpuKeyNameMem;
                                resultCopy.UploadId = testContext.test.uploadIdMem;
                                resultCopy.Parts =
                                 [{ PartNumber: 1,
                                     LastModified: res.Parts[0].LastModified,
                                     ETag: `"${normalMD5}"`,
                                     Size: normalBodySize }];
                                assert.deepStrictEqual(res, resultCopy);
                                next();
                            });
                        },
                    ], done);
                }
            );

            test('should copy part from GCP to MPU with AWS location', done => {
                const params = {
                    Bucket: memBucketName,
                    CopySource:
                      `${bucket}/${testContext.test.keyNameNormalGcp}`,
                    Key: testContext.test.mpuKeyNameAWS,
                    PartNumber: 1,
                    UploadId: testContext.test.uploadIdAWS,
                };
                async.waterfall([
                    next => s3.uploadPartCopy(params, (err, res) => {
                        assert.ifError(err, 'uploadPartCopy: Expected ' +
                        `success, got error: ${err}`);
                        expect(res.ETag).toBe(`"${normalMD5}"`);
                        next(err);
                    }),
                    next => {
                        const awsBucket =
                          config.locationConstraints[awsLocation]
                          .details.bucketName;
                        awsS3.listParts({
                            Bucket: awsBucket,
                            Key: testContext.test.mpuKeyNameAWS,
                            UploadId: testContext.test.uploadIdAWS,
                        }, (err, res) => {
                            assert.ifError(err,
                            'listParts: Expected success,' +
                            ` got error: ${err}`);
                            expect(res.Bucket).toBe(awsBucket);
                            expect(res.Key).toBe(testContext.test.mpuKeyNameAWS);
                            expect(res.UploadId).toBe(testContext.test.uploadIdAWS);
                            expect(res.Parts.length).toBe(1);
                            expect(res.Parts[0].PartNumber).toBe(1);
                            expect(res.Parts[0].ETag).toBe(`"${normalMD5}"`);
                            expect(res.Parts[0].Size).toBe(normalBodySize);
                            next();
                        });
                    },
                ], done);
            });

            test('should copy part from GCP object with range to MPU ' +
            'with AWS location', done => {
                const params = {
                    Bucket: memBucketName,
                    CopySource:
                      `${bucket}/${testContext.test.keyNameNormalGcp}`,
                    Key: testContext.test.mpuKeyNameAWS,
                    CopySourceRange: 'bytes=0-5',
                    PartNumber: 1,
                    UploadId: testContext.test.uploadIdAWS,
                };
                async.waterfall([
                    next => s3.uploadPartCopy(params, (err, res) => {
                        assert.ifError(err, 'uploadPartCopy: Expected ' +
                        `success, got error: ${err}`);
                        expect(res.ETag).toBe(`"${sixBytesMD5}"`);
                        next(err);
                    }),
                    next => {
                        const awsBucket =
                          config.locationConstraints[awsLocation]
                          .details.bucketName;
                        awsS3.listParts({
                            Bucket: awsBucket,
                            Key: testContext.test.mpuKeyNameAWS,
                            UploadId: testContext.test.uploadIdAWS,
                        }, (err, res) => {
                            assert.ifError(err,
                            'listParts: Expected success,' +
                            ` got error: ${err}`);
                            expect(res.Bucket).toBe(awsBucket);
                            expect(res.Key).toBe(testContext.test.mpuKeyNameAWS);
                            expect(res.UploadId).toBe(testContext.test.uploadIdAWS);
                            expect(res.Parts.length).toBe(1);
                            expect(res.Parts[0].PartNumber).toBe(1);
                            expect(res.Parts[0].ETag).toBe(`"${sixBytesMD5}"`);
                            expect(res.Parts[0].Size).toBe(6);
                            next();
                        });
                    },
                ], done);
            });

            test('should copy 5 Mb part from a memory location to MPU with ' +
            'GCP location', done => {
                const params = {
                    Bucket: bucket,
                    CopySource:
                      `${bucket}/${testContext.test.keyNameFiveMbMem}`,
                    Key: testContext.test.mpuKeyNameGcp,
                    PartNumber: 1,
                    UploadId: testContext.test.uploadId,
                };
                async.waterfall([
                    next => s3.uploadPartCopy(params, (err, res) => {
                        assert.ifError(err, 'uploadPartCopy: Expected ' +
                        `success, got error: ${err}`);
                        expect(res.ETag).toBe(`"${fiveMbMD5}"`);
                        next(err);
                    }),
                    next => {
                        const infos = {
                            bucketName: bucket,
                            keyName: testContext.test.mpuKeyNameGcp,
                            uploadId: testContext.test.uploadId,
                            md5: fiveMbMD5,
                            totalSize: fiveMB,
                        };
                        assertCopyPart(infos, next);
                    },
                ], done);
            });

            describe('with existing part', () => {
                beforeEach(done => {
                    const params = {
                        Body: oneKbBody,
                        Bucket: bucket,
                        Key: testContext.currentTest.mpuKeyNameGcp,
                        PartNumber: 1,
                        UploadId: testContext.currentTest.uploadId,
                    };
                    s3.uploadPart(params, done);
                });
                test('should copy part from GCP to GCP with existing ' +
                'parts', done => {
                    const resultCopy = JSON.parse(JSON.stringify(result));
                    const params = {
                        Bucket: bucket,
                        CopySource:
                        `${bucket}/${testContext.test.keyNameNormalGcp}`,
                        Key: testContext.test.mpuKeyNameGcp,
                        PartNumber: 2,
                        UploadId: testContext.test.uploadId,
                    };
                    async.waterfall([
                        next => s3.uploadPartCopy(params, (err, res) => {
                            assert.ifError(err,
                              'uploadPartCopy: Expected success, got ' +
                              `error: ${err}`);
                            expect(res.ETag).toBe(`"${normalMD5}"`);
                            next(err);
                        }),
                        next => s3.listParts({
                            Bucket: bucket,
                            Key: testContext.test.mpuKeyNameGcp,
                            UploadId: testContext.test.uploadId,
                        }, (err, res) => {
                            assert.ifError(err, 'listParts: Expected ' +
                            `success, got error: ${err}`);
                            resultCopy.Bucket = bucket;
                            resultCopy.Key = testContext.test.mpuKeyNameGcp;
                            resultCopy.UploadId = testContext.test.uploadId;
                            resultCopy.Parts =
                             [{ PartNumber: 1,
                                 LastModified: res.Parts[0].LastModified,
                                 ETag: `"${oneKbMD5}"`,
                                 Size: oneKb },
                               { PartNumber: 2,
                                   LastModified: res.Parts[1].LastModified,
                                   ETag: `"${normalMD5}"`,
                                   Size: 11 },
                             ];
                            assert.deepStrictEqual(res, resultCopy);
                            next();
                        }),
                        next => gcpClient.listParts({
                            Bucket: gcpBucketMPU,
                            Key: testContext.test.mpuKeyNameGcp,
                            UploadId: testContext.test.uploadId,
                        }, (err, res) => {
                            assert.ifError(err, 'GCP listParts: Expected ' +
                            `success, got error: ${err}`);
                            expect(res.Contents[0].ETag).toBe(`"${oneKbMD5}"`);
                            expect(res.Contents[1].ETag).toBe(`"${normalMD5}"`);
                            next();
                        }),
                    ], done);
                });
            });
        });
    });
});

describeSkipIfNotMultipleOrCeph('Put Copy Part to GCP with complete MPU',
function describeF() {
    this.timeout(800000);
    withV4(sigCfg => {
        beforeEach(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
        });

        afterEach(() => {
            process.stdout.write('Emptying bucket\n');
            return bucketUtil.empty(bucket)
            .then(() => {
                process.stdout.write('Deleting bucket\n');
                return bucketUtil.deleteOne(bucket);
            })
            .then(() => {
                process.stdout.write('Emptying bucket awsBucketName\n');
                return bucketUtil.empty(awsBucketName);
            })
            .then(() => {
                process.stdout.write('Deleting bucket awsBucketName\n');
                return bucketUtil.deleteOne(awsBucketName);
            })
            .catch(err => {
                process.stdout.write(`Error in afterEach: ${err}\n`);
                throw err;
            });
        });
        describe('Basic test with complete MPU from AWS to GCP location: ',
        () => {
            let testContext;

            beforeEach(() => {
                testContext = {};
            });

            beforeEach(done => {
                testContext.currentTest.keyNameAws =
                `onehundredandfivembgcp${uniqName(keyObjectAWS)}`;
                testContext.currentTest.mpuKeyNameGcp =
                `mpukeyname${uniqName(keyObjectGcp)}`;

                const createMpuParams = {
                    Bucket: bucket,
                    Key: testContext.currentTest.mpuKeyNameGcp,
                    Metadata: { 'scal-location-constraint': gcpLocation },
                };
                async.waterfall([
                    next => s3.createBucket({ Bucket: awsBucketName },
                      err => next(err)),
                    next => s3.createBucket({ Bucket: bucket },
                      err => next(err)),
                    next => s3.putObject({
                        Bucket: awsBucketName,
                        Key: testContext.currentTest.keyNameAws,
                        Body: fiveMbBody,
                        Metadata: { 'scal-location-constraint': awsLocation },
                    }, err => next(err)),
                    next => s3.createMultipartUpload(createMpuParams,
                    (err, res) => {
                        expect(err).toEqual(null);
                        testContext.currentTest.uploadId = res.UploadId;
                        next();
                    }),
                ], done);
            });

            test('should copy two 5 MB part from GCP to MPU with GCP' +
            'location', done => {
                const uploadParams = {
                    Bucket: bucket,
                    CopySource:
                      `${awsBucketName}/` +
                      `${testContext.test.keyNameAws}`,
                    Key: testContext.test.mpuKeyNameGcp,
                    PartNumber: 1,
                    UploadId: testContext.test.uploadId,
                };
                const uploadParams2 = {
                    Bucket: bucket,
                    CopySource:
                      `${awsBucketName}/` +
                      `${testContext.test.keyNameAws}`,
                    Key: testContext.test.mpuKeyNameGcp,
                    PartNumber: 2,
                    UploadId: testContext.test.uploadId,
                };
                async.waterfall([
                    next => s3.uploadPartCopy(uploadParams, (err, res) => {
                        expect(err).toEqual(null);
                        expect(res.ETag).toBe(`"${fiveMbMD5}"`);
                        next(err);
                    }),
                    next => s3.uploadPartCopy(uploadParams2, (err, res) => {
                        expect(err).toEqual(null);
                        expect(res.ETag).toBe(`"${fiveMbMD5}"`);
                        next(err);
                    }),
                    next => {
                        const completeMpuParams = {
                            Bucket: bucket,
                            Key: testContext.test.mpuKeyNameGcp,
                            MultipartUpload: {
                                Parts: [
                                    {
                                        ETag: `"${fiveMbMD5}"`,
                                        PartNumber: 1,
                                    },
                                    {
                                        ETag: `"${fiveMbMD5}"`,
                                        PartNumber: 2,
                                    },
                                ],
                            },
                            UploadId: testContext.test.uploadId,
                        };
                        s3.completeMultipartUpload(completeMpuParams,
                        (err, res) => {
                            expect(err).toEqual(null);
                            expect(res.Bucket).toBe(bucket);
                            expect(res.Key).toBe(testContext.test.mpuKeyNameGcp);
                            next();
                        });
                    },
                ], done);
            });
        });
    });
});
