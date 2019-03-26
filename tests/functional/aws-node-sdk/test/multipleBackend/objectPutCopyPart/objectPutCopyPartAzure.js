const async = require('async');
const assert = require('assert');
const { s3middleware } = require('arsenal');
const azureMpuUtils = s3middleware.azureHelper.mpuUtils;

const { config } = require('../../../../../../lib/Config');
const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const { uniqName, getAzureClient, azureLocation, azureLocationMismatch,
  memLocation, awsLocation, awsS3, getOwnerInfo, genUniqID }
  = require('../utils');

const describeSkipIfNotMultipleOrCeph = config.backends.data !== 'multiple'
    ? describe.skip : describe;

let azureContainerName;

if (config.locationConstraints[azureLocation] &&
config.locationConstraints[azureLocation].details &&
config.locationConstraints[azureLocation].details.azureContainerName) {
    azureContainerName =
      config.locationConstraints[azureLocation].details.azureContainerName;
}

const memBucketName = `memputcopypartazure${genUniqID()}`;
const awsBucketName = `awsputcopypartazure${genUniqID()}`;

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

const oneHundredAndFiveMB = 105 * 1024 * 1024;
const oneHundredAndFiveMbBody = Buffer.alloc(oneHundredAndFiveMB);
const oneHundredAndFiveMbMD5 = 'a9b59b0a5fe1ffed0b23fad2498c4dac';

const keyObjectAzure = 'objectputcopypartAzure';
const keyObjectMemory = 'objectputcopypartMemory';
const keyObjectAWS = 'objectputcopypartAWS';
const azureClient = getAzureClient();

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
    const { azureContainerName, mpuKeyNameAzure, uploadId, md5,
    subPartSize } = infos;
    const resultCopy = JSON.parse(JSON.stringify(result));
    resultCopy.Bucket = azureContainerName;
    resultCopy.Key = mpuKeyNameAzure;
    resultCopy.UploadId = uploadId;
    let totalSize = 0;
    for (let i = 0; i < subPartSize.length; i++) {
        totalSize = totalSize + subPartSize[i];
    }
    async.waterfall([
        next => s3.listParts({
            Bucket: azureContainerName,
            Key: mpuKeyNameAzure,
            UploadId: uploadId,
        }, (err, res) => {
            expect(err).toEqual(null);
            resultCopy.Parts =
             [{ PartNumber: 1,
                 LastModified: res.Parts[0].LastModified,
                 ETag: `"${md5}"`,
                 Size: totalSize }];
            assert.deepStrictEqual(res, resultCopy);
            next();
        }),
        next => azureClient.listBlocks(azureContainerName,
        mpuKeyNameAzure, 'all', (err, res) => {
            expect(err).toEqual(null);
            subPartSize.forEach((size, index) => {
                const partName = azureMpuUtils.getBlockId(uploadId, 1, index);
                expect(res.UncommittedBlocks[index].Name).toBe(partName);
                expect(res.UncommittedBlocks[index].Size).toEqual(size);
            });
            next();
        }),
    ], cb);
}

describeSkipIfNotMultipleOrCeph('Put Copy Part to AZURE', function describeF() {
    this.timeout(800000);
    withV4(sigCfg => {
        beforeEach(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
        });

        afterEach(() => {
            process.stdout.write('Emptying bucket\n');
            return bucketUtil.empty(azureContainerName)
            .then(() => bucketUtil.empty(memBucketName))
            .then(() => {
                process.stdout.write(`Deleting bucket ${azureContainerName}\n`);
                return bucketUtil.deleteOne(azureContainerName);
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
                testContext.currentTest.keyNameNormalAzure =
                `normalazure${uniqName(keyObjectAzure)}`;
                testContext.currentTest.keyNameNormalAzureMismatch =
                `normalazuremismatch${uniqName(keyObjectAzure)}`;

                testContext.currentTest.keyNameFiveMbAzure =
                `fivembazure${uniqName(keyObjectAzure)}`;
                testContext.currentTest.keyNameFiveMbMem =
                `fivembmem${uniqName(keyObjectMemory)}`;

                testContext.currentTest.mpuKeyNameAzure =
                `mpukeyname${uniqName(keyObjectAzure)}`;
                testContext.currentTest.mpuKeyNameMem =
                `mpukeyname${uniqName(keyObjectMemory)}`;
                testContext.currentTest.mpuKeyNameAWS =
                `mpukeyname${uniqName(keyObjectAWS)}`;
                const paramsAzure = {
                    Bucket: azureContainerName,
                    Key: testContext.currentTest.mpuKeyNameAzure,
                    Metadata: { 'scal-location-constraint': azureLocation },
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
                    next => s3.createBucket({ Bucket: azureContainerName },
                      err => next(err)),
                    next => s3.createBucket({ Bucket: memBucketName },
                      err => next(err)),
                    next => s3.putObject({
                        Bucket: azureContainerName,
                        Key: testContext.currentTest.keyNameNormalAzure,
                        Body: normalBody,
                        Metadata: { 'scal-location-constraint': azureLocation },
                    }, err => next(err)),
                    next => s3.putObject({
                        Bucket: azureContainerName,
                        Key: testContext.currentTest.keyNameNormalAzureMismatch,
                        Body: normalBody,
                        Metadata: { 'scal-location-constraint':
                        azureLocationMismatch },
                    }, err => next(err)),
                    next => s3.putObject({
                        Bucket: azureContainerName,
                        Key: testContext.currentTest.keyNameFiveMbAzure,
                        Body: fiveMbBody,
                        Metadata: { 'scal-location-constraint': azureLocation },
                    }, err => next(err)),
                    next => s3.putObject({
                        Bucket: azureContainerName,
                        Key: testContext.currentTest.keyNameFiveMbMem,
                        Body: fiveMbBody,
                        Metadata: { 'scal-location-constraint': memLocation },
                    }, err => next(err)),
                    next => s3.createMultipartUpload(paramsAzure,
                    (err, res) => {
                        expect(err).toEqual(null);
                        testContext.currentTest.uploadId = res.UploadId;
                        next();
                    }),
                    next => s3.createMultipartUpload(paramsMem,
                    (err, res) => {
                        expect(err).toEqual(null);
                        testContext.currentTest.uploadIdMem = res.UploadId;
                        next();
                    }),
                    next => s3.createMultipartUpload(paramsAWS,
                    (err, res) => {
                        expect(err).toEqual(null);
                        testContext.currentTest.uploadIdAWS = res.UploadId;
                        next();
                    }),
                ], done);
            });
            afterEach(done => {
                const paramsAzure = {
                    Bucket: azureContainerName,
                    Key: testContext.currentTest.mpuKeyNameAzure,
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
                    next => s3.abortMultipartUpload(paramsAzure,
                      err => next(err)),
                    next => s3.abortMultipartUpload(paramsMem,
                      err => next(err)),
                    next => s3.abortMultipartUpload(paramsAWS,
                      err => next(err)),
                ], done);
            });
            test(
                'should copy small part from Azure to MPU with Azure location',
                done => {
                    const params = {
                        Bucket: azureContainerName,
                        CopySource:
                          `${azureContainerName}/${testContext.test.keyNameNormalAzure}`,
                        Key: testContext.test.mpuKeyNameAzure,
                        PartNumber: 1,
                        UploadId: testContext.test.uploadId,
                    };
                    async.waterfall([
                        next => s3.uploadPartCopy(params, (err, res) => {
                            expect(err).toEqual(null);
                            expect(res.ETag).toBe(`"${normalMD5}"`);
                            next(err);
                        }),
                        next => {
                            const infos = {
                                azureContainerName,
                                mpuKeyNameAzure: testContext.test.mpuKeyNameAzure,
                                uploadId: testContext.test.uploadId,
                                md5: normalMD5,
                                subPartSize: [normalBodySize],
                            };
                            assertCopyPart(infos, next);
                        },
                    ], done);
                }
            );

            test('should copy small part from Azure location with ' +
            'bucketMatch=false to MPU with Azure location', done => {
                const params = {
                    Bucket: azureContainerName,
                    CopySource:
                      `${azureContainerName}/` +
                      `${testContext.test.keyNameNormalAzureMismatch}`,
                    Key: testContext.test.mpuKeyNameAzure,
                    PartNumber: 1,
                    UploadId: testContext.test.uploadId,
                };
                async.waterfall([
                    next => s3.uploadPartCopy(params, (err, res) => {
                        expect(err).toEqual(null);
                        expect(res.ETag).toBe(`"${normalMD5}"`);
                        next(err);
                    }),
                    next => {
                        const infos = {
                            azureContainerName,
                            mpuKeyNameAzure: testContext.test.mpuKeyNameAzure,
                            uploadId: testContext.test.uploadId,
                            md5: normalMD5,
                            subPartSize: [normalBodySize],
                        };
                        assertCopyPart(infos, next);
                    },
                ], done);
            });

            test(
                'should copy 5 Mb part from Azure to MPU with Azure location',
                done => {
                    const params = {
                        Bucket: azureContainerName,
                        CopySource:
                          `${azureContainerName}/${testContext.test.keyNameFiveMbAzure}`,
                        Key: testContext.test.mpuKeyNameAzure,
                        PartNumber: 1,
                        UploadId: testContext.test.uploadId,
                    };
                    async.waterfall([
                        next => s3.uploadPartCopy(params, (err, res) => {
                            expect(err).toEqual(null);
                            expect(res.ETag).toBe(`"${fiveMbMD5}"`);
                            next(err);
                        }),
                        next => {
                            const infos = {
                                azureContainerName,
                                mpuKeyNameAzure: testContext.test.mpuKeyNameAzure,
                                uploadId: testContext.test.uploadId,
                                md5: fiveMbMD5,
                                subPartSize: [fiveMB],
                            };
                            assertCopyPart(infos, next);
                        },
                    ], done);
                }
            );

            test(
                'should copy part from Azure to MPU with memory location',
                done => {
                    const params = {
                        Bucket: memBucketName,
                        CopySource:
                          `${azureContainerName}/${testContext.test.keyNameNormalAzure}`,
                        Key: testContext.test.mpuKeyNameMem,
                        PartNumber: 1,
                        UploadId: testContext.test.uploadIdMem,
                    };
                    async.waterfall([
                        next => s3.uploadPartCopy(params, (err, res) => {
                            expect(err).toEqual(null);
                            expect(res.ETag).toBe(`"${normalMD5}"`);
                            next(err);
                        }),
                        next => {
                            s3.listParts({
                                Bucket: memBucketName,
                                Key: testContext.test.mpuKeyNameMem,
                                UploadId: testContext.test.uploadIdMem,
                            }, (err, res) => {
                                expect(err).toEqual(null);
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

            test(
                'should copy part from Azure to MPU with AWS location',
                done => {
                    const params = {
                        Bucket: memBucketName,
                        CopySource:
                          `${azureContainerName}/${testContext.test.keyNameNormalAzure}`,
                        Key: testContext.test.mpuKeyNameAWS,
                        PartNumber: 1,
                        UploadId: testContext.test.uploadIdAWS,
                    };
                    async.waterfall([
                        next => s3.uploadPartCopy(params, (err, res) => {
                            expect(err).toEqual(null);
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
                                expect(err).toEqual(null);
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
                }
            );

            test('should copy part from Azure object with range to MPU ' +
            'with AWS location', done => {
                const params = {
                    Bucket: memBucketName,
                    CopySource:
                      `${azureContainerName}/${testContext.test.keyNameNormalAzure}`,
                    Key: testContext.test.mpuKeyNameAWS,
                    CopySourceRange: 'bytes=0-5',
                    PartNumber: 1,
                    UploadId: testContext.test.uploadIdAWS,
                };
                async.waterfall([
                    next => s3.uploadPartCopy(params, (err, res) => {
                        expect(err).toEqual(null);
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
                            expect(err).toEqual(null);
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
            'Azure location', done => {
                const params = {
                    Bucket: azureContainerName,
                    CopySource:
                      `${azureContainerName}/${testContext.test.keyNameFiveMbMem}`,
                    Key: testContext.test.mpuKeyNameAzure,
                    PartNumber: 1,
                    UploadId: testContext.test.uploadId,
                };
                async.waterfall([
                    next => s3.uploadPartCopy(params, (err, res) => {
                        expect(err).toEqual(null);
                        expect(res.ETag).toBe(`"${fiveMbMD5}"`);
                        next(err);
                    }),
                    next => {
                        const infos = {
                            azureContainerName,
                            mpuKeyNameAzure: testContext.test.mpuKeyNameAzure,
                            uploadId: testContext.test.uploadId,
                            md5: fiveMbMD5,
                            subPartSize: [fiveMB],
                        };
                        assertCopyPart(infos, next);
                    },
                ], done);
            });

            describe('with existing part', () => {
                beforeEach(done => {
                    const params = {
                        Body: oneKbBody,
                        Bucket: azureContainerName,
                        Key: testContext.currentTest.mpuKeyNameAzure,
                        PartNumber: 1,
                        UploadId: testContext.currentTest.uploadId,
                    };
                    s3.uploadPart(params, done);
                });
                test('should copy part from Azure to Azure with existing ' +
                'parts', done => {
                    const resultCopy = JSON.parse(JSON.stringify(result));
                    const params = {
                        Bucket: azureContainerName,
                        CopySource:
                        `${azureContainerName}/${testContext.test.keyNameNormalAzure}`,
                        Key: testContext.test.mpuKeyNameAzure,
                        PartNumber: 2,
                        UploadId: testContext.test.uploadId,
                    };
                    async.waterfall([
                        next => s3.uploadPartCopy(params, (err, res) => {
                            expect(err).toEqual(null);
                            expect(res.ETag).toBe(`"${normalMD5}"`);
                            next(err);
                        }),
                        next => s3.listParts({
                            Bucket: azureContainerName,
                            Key: testContext.test.mpuKeyNameAzure,
                            UploadId: testContext.test.uploadId,
                        }, (err, res) => {
                            expect(err).toEqual(null);
                            resultCopy.Bucket = azureContainerName;
                            resultCopy.Key = testContext.test.mpuKeyNameAzure;
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
                        next => azureClient.listBlocks(azureContainerName,
                        testContext.test.mpuKeyNameAzure, 'all', (err, res) => {
                            expect(err).toEqual(null);
                            const partName = azureMpuUtils.getBlockId(
                              testContext.test.uploadId, 1, 0);
                            const partName2 = azureMpuUtils.getBlockId(
                              testContext.test.uploadId, 2, 0);
                            expect(res.UncommittedBlocks[0].Name).toBe(partName);
                            expect(res.UncommittedBlocks[0].Size).toEqual(oneKb);
                            expect(res.UncommittedBlocks[1].Name).toBe(partName2);
                            expect(res.UncommittedBlocks[1].Size).toEqual(11);
                            next();
                        }),
                    ], done);
                });
            });
        });
    });
});

describeSkipIfNotMultipleOrCeph('Put Copy Part to AZURE with large object',
function describeF() {
    this.timeout(800000);
    withV4(sigCfg => {
        beforeEach(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
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
        describe('Basic test with large object: ', () => {
            let testContext;

            beforeEach(() => {
                testContext = {};
            });

            beforeEach(done => {
                testContext.currentTest.keyNameOneHundredAndFiveMbAzure =
                `onehundredandfivembazure${uniqName(keyObjectAzure)}`;
                testContext.currentTest.mpuKeyNameAzure =
                `mpukeyname${uniqName(keyObjectAzure)}`;

                const params = {
                    Bucket: azureContainerName,
                    Key: testContext.currentTest.mpuKeyNameAzure,
                    Metadata: { 'scal-location-constraint': azureLocation },
                };
                async.waterfall([
                    next => s3.createBucket({ Bucket: azureContainerName },
                      err => next(err)),
                    next => s3.putObject({
                        Bucket: azureContainerName,
                        Key: testContext.currentTest.keyNameOneHundredAndFiveMbAzure,
                        Body: oneHundredAndFiveMbBody,
                        Metadata: { 'scal-location-constraint': azureLocation },
                    }, err => next(err)),
                    next => s3.createMultipartUpload(params, (err, res) => {
                        expect(err).toEqual(null);
                        testContext.currentTest.uploadId = res.UploadId;
                        next();
                    }),
                ], done);
            });
            afterEach(done => {
                const params = {
                    Bucket: azureContainerName,
                    Key: testContext.currentTest.mpuKeyNameAzure,
                    UploadId: testContext.currentTest.uploadId,
                };
                s3.abortMultipartUpload(params, done);
            });

            test('should copy 105 MB part from Azure to MPU with Azure ' +
            'location', done => {
                const params = {
                    Bucket: azureContainerName,
                    CopySource:
                      `${azureContainerName}/` +
                      `${testContext.test.keyNameOneHundredAndFiveMbAzure}`,
                    Key: testContext.test.mpuKeyNameAzure,
                    PartNumber: 1,
                    UploadId: testContext.test.uploadId,
                };
                async.waterfall([
                    next => s3.uploadPartCopy(params, (err, res) => {
                        expect(err).toEqual(null);
                        expect(res.ETag).toBe(`"${oneHundredAndFiveMbMD5}"`);
                        next(err);
                    }),
                    next => {
                        const infos = {
                            azureContainerName,
                            mpuKeyNameAzure:
                            testContext.test.mpuKeyNameAzure,
                            uploadId: testContext.test.uploadId,
                            md5: oneHundredAndFiveMbMD5,
                            subPartSize: [100 * 1024 * 1024, 5 * 1024 * 1024],
                        };
                        assertCopyPart(infos, next);
                    },
                ], done);
            });
        });
    });
});

describeSkipIfNotMultipleOrCeph('Put Copy Part to AZURE with complete MPU',
function describeF() {
    this.timeout(800000);
    withV4(sigCfg => {
        beforeEach(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
        });

        afterEach(() => {
            process.stdout.write('Emptying bucket azureContainerName\n');
            return bucketUtil.empty(azureContainerName)
            .then(() => {
                process.stdout.write('Deleting bucket azureContainerName\n');
                return bucketUtil.deleteOne(azureContainerName);
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
        describe('Basic test with complete MPU from AWS to Azure location: ',
        () => {
            let testContext;

            beforeEach(() => {
                testContext = {};
            });

            beforeEach(done => {
                testContext.currentTest.keyNameAws =
                `onehundredandfivembazure${uniqName(keyObjectAWS)}`;
                testContext.currentTest.mpuKeyNameAzure =
                `mpukeyname${uniqName(keyObjectAzure)}`;

                const createMpuParams = {
                    Bucket: azureContainerName,
                    Key: testContext.currentTest.mpuKeyNameAzure,
                    Metadata: { 'scal-location-constraint': azureLocation },
                };
                async.waterfall([
                    next => s3.createBucket({ Bucket: awsBucketName },
                      err => next(err)),
                    next => s3.createBucket({ Bucket: azureContainerName },
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

            test('should copy two 5 MB part from Azure to MPU with Azure ' +
            'location', done => {
                const uploadParams = {
                    Bucket: azureContainerName,
                    CopySource:
                      `${awsBucketName}/` +
                      `${testContext.test.keyNameAws}`,
                    Key: testContext.test.mpuKeyNameAzure,
                    PartNumber: 1,
                    UploadId: testContext.test.uploadId,
                };
                const uploadParams2 = {
                    Bucket: azureContainerName,
                    CopySource:
                      `${awsBucketName}/` +
                      `${testContext.test.keyNameAws}`,
                    Key: testContext.test.mpuKeyNameAzure,
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
                            Bucket: azureContainerName,
                            Key: testContext.test.mpuKeyNameAzure,
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
                            expect(res.Bucket).toBe(azureContainerName);
                            expect(res.Key).toBe(testContext.test.mpuKeyNameAzure);
                            next();
                        });
                    },
                ], done);
            });
        });
    });
});
