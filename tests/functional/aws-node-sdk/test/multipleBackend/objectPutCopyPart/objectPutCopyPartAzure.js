const async = require('async');
const assert = require('assert');
const { s3middleware } = require('arsenal');
const azureMpuUtils = s3middleware.azureHelper.mpuUtils;

const { config } = require('../../../../../../lib/Config');
const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const { uniqName, getAzureClient, azureLocation, azureLocationMismatch,
  memLocation, awsLocation, awsS3, getOwnerInfo } = require('../utils');

const describeSkipIfNotMultiple = config.backends.data !== 'multiple'
    ? describe.skip : describe;

let azureContainerName;

if (config.locationConstraints[azureLocation] &&
config.locationConstraints[azureLocation].details &&
config.locationConstraints[azureLocation].details.azureContainerName) {
    azureContainerName =
      config.locationConstraints[azureLocation].details.azureContainerName;
}

const memBucketName = 'membucketnameputcopypartazure';

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
            assert.equal(err, null, 'listParts: Expected success,' +
            ` got error: ${err}`);
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
            assert.equal(err, null, 'listBlocks: Expected ' +
            `success, got error: ${err}`);
            subPartSize.forEach((size, index) => {
                const partName = azureMpuUtils.getBlockId(uploadId, 1, index);
                assert.strictEqual(res.UncommittedBlocks[index].Name,
                  partName);
                assert.equal(res.UncommittedBlocks[index].Size, size);
            });
            next();
        }),
    ], cb);
}

describeSkipIfNotMultiple('Put Copy Part to AZURE', function describeF() {
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
            beforeEach(function beF(done) {
                this.currentTest.keyNameNormalAzure =
                `normalazure${uniqName(keyObjectAzure)}`;
                this.currentTest.keyNameNormalAzureMismatch =
                `normalazuremismatch${uniqName(keyObjectAzure)}`;

                this.currentTest.keyNameFiveMbAzure =
                `fivembazure${uniqName(keyObjectAzure)}`;
                this.currentTest.keyNameFiveMbMem =
                `fivembmem${uniqName(keyObjectMemory)}`;

                this.currentTest.mpuKeyNameAzure =
                `mpukeyname${uniqName(keyObjectAzure)}`;
                this.currentTest.mpuKeyNameMem =
                `mpukeyname${uniqName(keyObjectMemory)}`;
                this.currentTest.mpuKeyNameAWS =
                `mpukeyname${uniqName(keyObjectAWS)}`;
                const paramsAzure = {
                    Bucket: azureContainerName,
                    Key: this.currentTest.mpuKeyNameAzure,
                    Metadata: { 'scal-location-constraint': azureLocation },
                };
                const paramsMem = {
                    Bucket: memBucketName,
                    Key: this.currentTest.mpuKeyNameMem,
                    Metadata: { 'scal-location-constraint': memLocation },
                };
                const paramsAWS = {
                    Bucket: memBucketName,
                    Key: this.currentTest.mpuKeyNameAWS,
                    Metadata: { 'scal-location-constraint': awsLocation },
                };
                async.waterfall([
                    next => s3.createBucket({ Bucket: azureContainerName },
                      err => next(err)),
                    next => s3.createBucket({ Bucket: memBucketName },
                      err => next(err)),
                    next => s3.putObject({
                        Bucket: azureContainerName,
                        Key: this.currentTest.keyNameNormalAzure,
                        Body: normalBody,
                        Metadata: { 'scal-location-constraint': azureLocation },
                    }, err => next(err)),
                    next => s3.putObject({
                        Bucket: azureContainerName,
                        Key: this.currentTest.keyNameNormalAzureMismatch,
                        Body: normalBody,
                        Metadata: { 'scal-location-constraint':
                        azureLocationMismatch },
                    }, err => next(err)),
                    next => s3.putObject({
                        Bucket: azureContainerName,
                        Key: this.currentTest.keyNameFiveMbAzure,
                        Body: fiveMbBody,
                        Metadata: { 'scal-location-constraint': azureLocation },
                    }, err => next(err)),
                    next => s3.putObject({
                        Bucket: azureContainerName,
                        Key: this.currentTest.keyNameFiveMbMem,
                        Body: fiveMbBody,
                        Metadata: { 'scal-location-constraint': memLocation },
                    }, err => next(err)),
                    next => s3.createMultipartUpload(paramsAzure,
                    (err, res) => {
                        assert.equal(err, null, 'createMultipartUpload ' +
                        `on Azure: Expected success, got error: ${err}`);
                        this.currentTest.uploadId = res.UploadId;
                        next();
                    }),
                    next => s3.createMultipartUpload(paramsMem,
                    (err, res) => {
                        assert.equal(err, null, 'createMultipartUpload ' +
                        `in memory: Expected success, got error: ${err}`);
                        this.currentTest.uploadIdMem = res.UploadId;
                        next();
                    }),
                    next => s3.createMultipartUpload(paramsAWS,
                    (err, res) => {
                        assert.equal(err, null, 'createMultipartUpload ' +
                        `on AWS: Expected success, got error: ${err}`);
                        this.currentTest.uploadIdAWS = res.UploadId;
                        next();
                    }),
                ], done);
            });
            afterEach(function afterEachF(done) {
                const paramsAzure = {
                    Bucket: azureContainerName,
                    Key: this.currentTest.mpuKeyNameAzure,
                    UploadId: this.currentTest.uploadId,
                };
                const paramsMem = {
                    Bucket: memBucketName,
                    Key: this.currentTest.mpuKeyNameMem,
                    UploadId: this.currentTest.uploadIdMem,
                };
                const paramsAWS = {
                    Bucket: memBucketName,
                    Key: this.currentTest.mpuKeyNameAWS,
                    UploadId: this.currentTest.uploadIdAWS,
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
            it('should copy small part from Azure to MPU with Azure location',
            function ifF(done) {
                const params = {
                    Bucket: azureContainerName,
                    CopySource:
                      `${azureContainerName}/${this.test.keyNameNormalAzure}`,
                    Key: this.test.mpuKeyNameAzure,
                    PartNumber: 1,
                    UploadId: this.test.uploadId,
                };
                async.waterfall([
                    next => s3.uploadPartCopy(params, (err, res) => {
                        assert.equal(err, null, 'uploadPartCopy: Expected ' +
                        `success, got error: ${err}`);
                        assert.strictEqual(res.ETag, `"${normalMD5}"`);
                        next(err);
                    }),
                    next => {
                        const infos = {
                            azureContainerName,
                            mpuKeyNameAzure: this.test.mpuKeyNameAzure,
                            uploadId: this.test.uploadId,
                            md5: normalMD5,
                            subPartSize: [normalBodySize],
                        };
                        assertCopyPart(infos, next);
                    },
                ], done);
            });

            it('should copy small part from Azure location with ' +
            'bucketMatch=false to MPU with Azure location',
            function ifF(done) {
                const params = {
                    Bucket: azureContainerName,
                    CopySource:
                      `${azureContainerName}/` +
                      `${this.test.keyNameNormalAzureMismatch}`,
                    Key: this.test.mpuKeyNameAzure,
                    PartNumber: 1,
                    UploadId: this.test.uploadId,
                };
                async.waterfall([
                    next => s3.uploadPartCopy(params, (err, res) => {
                        assert.equal(err, null, 'uploadPartCopy: Expected ' +
                        `success, got error: ${err}`);
                        assert.strictEqual(res.ETag, `"${normalMD5}"`);
                        next(err);
                    }),
                    next => {
                        const infos = {
                            azureContainerName,
                            mpuKeyNameAzure: this.test.mpuKeyNameAzure,
                            uploadId: this.test.uploadId,
                            md5: normalMD5,
                            subPartSize: [normalBodySize],
                        };
                        assertCopyPart(infos, next);
                    },
                ], done);
            });

            it('should copy 5 Mb part from Azure to MPU with Azure location',
            function ifF(done) {
                const params = {
                    Bucket: azureContainerName,
                    CopySource:
                      `${azureContainerName}/${this.test.keyNameFiveMbAzure}`,
                    Key: this.test.mpuKeyNameAzure,
                    PartNumber: 1,
                    UploadId: this.test.uploadId,
                };
                async.waterfall([
                    next => s3.uploadPartCopy(params, (err, res) => {
                        assert.equal(err, null, 'uploadPartCopy: Expected ' +
                        `success, got error: ${err}`);
                        assert.strictEqual(res.ETag, `"${fiveMbMD5}"`);
                        next(err);
                    }),
                    next => {
                        const infos = {
                            azureContainerName,
                            mpuKeyNameAzure: this.test.mpuKeyNameAzure,
                            uploadId: this.test.uploadId,
                            md5: fiveMbMD5,
                            subPartSize: [fiveMB],
                        };
                        assertCopyPart(infos, next);
                    },
                ], done);
            });

            it('should copy part from Azure to MPU with memory location',
            function ifF(done) {
                const params = {
                    Bucket: memBucketName,
                    CopySource:
                      `${azureContainerName}/${this.test.keyNameNormalAzure}`,
                    Key: this.test.mpuKeyNameMem,
                    PartNumber: 1,
                    UploadId: this.test.uploadIdMem,
                };
                async.waterfall([
                    next => s3.uploadPartCopy(params, (err, res) => {
                        assert.equal(err, null, 'uploadPartCopy: Expected ' +
                        `success, got error: ${err}`);
                        assert.strictEqual(res.ETag, `"${normalMD5}"`);
                        next(err);
                    }),
                    next => {
                        s3.listParts({
                            Bucket: memBucketName,
                            Key: this.test.mpuKeyNameMem,
                            UploadId: this.test.uploadIdMem,
                        }, (err, res) => {
                            assert.equal(err, null,
                            'listParts: Expected success,' +
                            ` got error: ${err}`);
                            const resultCopy =
                            JSON.parse(JSON.stringify(result));
                            resultCopy.Bucket = memBucketName;
                            resultCopy.Key = this.test.mpuKeyNameMem;
                            resultCopy.UploadId = this.test.uploadIdMem;
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
            });

            it('should copy part from Azure to MPU with AWS location',
            function ifF(done) {
                const params = {
                    Bucket: memBucketName,
                    CopySource:
                      `${azureContainerName}/${this.test.keyNameNormalAzure}`,
                    Key: this.test.mpuKeyNameAWS,
                    PartNumber: 1,
                    UploadId: this.test.uploadIdAWS,
                };
                async.waterfall([
                    next => s3.uploadPartCopy(params, (err, res) => {
                        assert.equal(err, null, 'uploadPartCopy: Expected ' +
                        `success, got error: ${err}`);
                        assert.strictEqual(res.ETag, `"${normalMD5}"`);
                        next(err);
                    }),
                    next => {
                        const awsBucket =
                          config.locationConstraints[awsLocation]
                          .details.bucketName;
                        awsS3.listParts({
                            Bucket: awsBucket,
                            Key: this.test.mpuKeyNameAWS,
                            UploadId: this.test.uploadIdAWS,
                        }, (err, res) => {
                            assert.equal(err, null,
                            'listParts: Expected success,' +
                            ` got error: ${err}`);
                            assert.strictEqual(res.Bucket, awsBucket);
                            assert.strictEqual(res.Key,
                              this.test.mpuKeyNameAWS);
                            assert.strictEqual(res.UploadId,
                              this.test.uploadIdAWS);
                            assert.strictEqual(res.Parts.length, 1);
                            assert.strictEqual(res.Parts[0].PartNumber, 1);
                            assert.strictEqual(res.Parts[0].ETag,
                              `"${normalMD5}"`);
                            assert.strictEqual(res.Parts[0].Size,
                              normalBodySize);
                            next();
                        });
                    },
                ], done);
            });

            it('should copy part from Azure object with range to MPU ' +
            'with AWS location', function ifF(done) {
                const params = {
                    Bucket: memBucketName,
                    CopySource:
                      `${azureContainerName}/${this.test.keyNameNormalAzure}`,
                    Key: this.test.mpuKeyNameAWS,
                    CopySourceRange: 'bytes=0-5',
                    PartNumber: 1,
                    UploadId: this.test.uploadIdAWS,
                };
                async.waterfall([
                    next => s3.uploadPartCopy(params, (err, res) => {
                        assert.equal(err, null, 'uploadPartCopy: Expected ' +
                        `success, got error: ${err}`);
                        assert.strictEqual(res.ETag, `"${sixBytesMD5}"`);
                        next(err);
                    }),
                    next => {
                        const awsBucket =
                          config.locationConstraints[awsLocation]
                          .details.bucketName;
                        awsS3.listParts({
                            Bucket: awsBucket,
                            Key: this.test.mpuKeyNameAWS,
                            UploadId: this.test.uploadIdAWS,
                        }, (err, res) => {
                            assert.equal(err, null,
                            'listParts: Expected success,' +
                            ` got error: ${err}`);
                            assert.strictEqual(res.Bucket, awsBucket);
                            assert.strictEqual(res.Key,
                              this.test.mpuKeyNameAWS);
                            assert.strictEqual(res.UploadId,
                              this.test.uploadIdAWS);
                            assert.strictEqual(res.Parts.length, 1);
                            assert.strictEqual(res.Parts[0].PartNumber, 1);
                            assert.strictEqual(res.Parts[0].ETag,
                              `"${sixBytesMD5}"`);
                            assert.strictEqual(res.Parts[0].Size, 6);
                            next();
                        });
                    },
                ], done);
            });

            it('should copy 5 Mb part from a memory location to MPU with ' +
            'Azure location',
            function ifF(done) {
                const params = {
                    Bucket: azureContainerName,
                    CopySource:
                      `${azureContainerName}/${this.test.keyNameFiveMbMem}`,
                    Key: this.test.mpuKeyNameAzure,
                    PartNumber: 1,
                    UploadId: this.test.uploadId,
                };
                async.waterfall([
                    next => s3.uploadPartCopy(params, (err, res) => {
                        assert.equal(err, null, 'uploadPartCopy: Expected ' +
                        `success, got error: ${err}`);
                        assert.strictEqual(res.ETag, `"${fiveMbMD5}"`);
                        next(err);
                    }),
                    next => {
                        const infos = {
                            azureContainerName,
                            mpuKeyNameAzure: this.test.mpuKeyNameAzure,
                            uploadId: this.test.uploadId,
                            md5: fiveMbMD5,
                            subPartSize: [fiveMB],
                        };
                        assertCopyPart(infos, next);
                    },
                ], done);
            });

            describe('with existing part', () => {
                beforeEach(function beF(done) {
                    const params = {
                        Body: oneKbBody,
                        Bucket: azureContainerName,
                        Key: this.currentTest.mpuKeyNameAzure,
                        PartNumber: 1,
                        UploadId: this.currentTest.uploadId,
                    };
                    s3.uploadPart(params, done);
                });
                it('should copy part from Azure to Azure with existing ' +
                'parts', function ifF(done) {
                    const resultCopy = JSON.parse(JSON.stringify(result));
                    const params = {
                        Bucket: azureContainerName,
                        CopySource:
                        `${azureContainerName}/${this.test.keyNameNormalAzure}`,
                        Key: this.test.mpuKeyNameAzure,
                        PartNumber: 2,
                        UploadId: this.test.uploadId,
                    };
                    async.waterfall([
                        next => s3.uploadPartCopy(params, (err, res) => {
                            assert.equal(err, null,
                              'uploadPartCopy: Expected success, got ' +
                              `error: ${err}`);
                            assert.strictEqual(res.ETag, `"${normalMD5}"`);
                            next(err);
                        }),
                        next => s3.listParts({
                            Bucket: azureContainerName,
                            Key: this.test.mpuKeyNameAzure,
                            UploadId: this.test.uploadId,
                        }, (err, res) => {
                            assert.equal(err, null, 'listParts: Expected ' +
                            `success, got error: ${err}`);
                            resultCopy.Bucket = azureContainerName;
                            resultCopy.Key = this.test.mpuKeyNameAzure;
                            resultCopy.UploadId = this.test.uploadId;
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
                        this.test.mpuKeyNameAzure, 'all', (err, res) => {
                            assert.equal(err, null, 'listBlocks: Expected ' +
                            `success, got error: ${err}`);
                            const partName = azureMpuUtils.getBlockId(
                              this.test.uploadId, 1, 0);
                            const partName2 = azureMpuUtils.getBlockId(
                              this.test.uploadId, 2, 0);
                            assert.strictEqual(res.UncommittedBlocks[0].Name,
                              partName);
                            assert.equal(res.UncommittedBlocks[0].Size,
                            oneKb);
                            assert.strictEqual(res.UncommittedBlocks[1].Name,
                                partName2);
                            assert.equal(res.UncommittedBlocks[1].Size,
                            11);
                            next();
                        }),
                    ], done);
                });
            });
        });
    });
});

describeSkipIfNotMultiple('Put Copy Part to AZURE with large object',
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
            beforeEach(function beF(done) {
                this.currentTest.keyNameOneHundredAndFiveMbAzure =
                `onehundredandfivembazure${uniqName(keyObjectAzure)}`;
                this.currentTest.mpuKeyNameAzure =
                `mpukeyname${uniqName(keyObjectAzure)}`;

                const params = {
                    Bucket: azureContainerName,
                    Key: this.currentTest.mpuKeyNameAzure,
                    Metadata: { 'scal-location-constraint': azureLocation },
                };
                async.waterfall([
                    next => s3.createBucket({ Bucket: azureContainerName },
                      err => next(err)),
                    next => s3.putObject({
                        Bucket: azureContainerName,
                        Key: this.currentTest.keyNameOneHundredAndFiveMbAzure,
                        Body: oneHundredAndFiveMbBody,
                        Metadata: { 'scal-location-constraint': azureLocation },
                    }, err => next(err)),
                    next => s3.createMultipartUpload(params, (err, res) => {
                        assert.equal(err, null, 'createMultipartUpload: ' +
                        `Expected success, got error: ${err}`);
                        this.currentTest.uploadId = res.UploadId;
                        next();
                    }),
                ], done);
            });
            afterEach(function afterEachF(done) {
                const params = {
                    Bucket: azureContainerName,
                    Key: this.currentTest.mpuKeyNameAzure,
                    UploadId: this.currentTest.uploadId,
                };
                s3.abortMultipartUpload(params, done);
            });

            it('should copy 105 MB part from Azure to MPU with Azure ' +
            'location', function ifF(done) {
                const params = {
                    Bucket: azureContainerName,
                    CopySource:
                      `${azureContainerName}/` +
                      `${this.test.keyNameOneHundredAndFiveMbAzure}`,
                    Key: this.test.mpuKeyNameAzure,
                    PartNumber: 1,
                    UploadId: this.test.uploadId,
                };
                async.waterfall([
                    next => s3.uploadPartCopy(params, (err, res) => {
                        assert.equal(err, null, 'uploadPartCopy: Expected ' +
                        `success, got error: ${err}`);
                        assert.strictEqual(res.ETag,
                        `"${oneHundredAndFiveMbMD5}"`);
                        next(err);
                    }),
                    next => {
                        const infos = {
                            azureContainerName,
                            mpuKeyNameAzure:
                            this.test.mpuKeyNameAzure,
                            uploadId: this.test.uploadId,
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
