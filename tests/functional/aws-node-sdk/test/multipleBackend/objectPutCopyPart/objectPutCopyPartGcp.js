const async = require('async');
const assert = require('assert');

const {
    CreateBucketCommand,
    PutObjectCommand,
    CreateMultipartUploadCommand,
    UploadPartCopyCommand,
    UploadPartCommand,
    ListPartsCommand,
    AbortMultipartUploadCommand,
    CompleteMultipartUploadCommand,
} = require('@aws-sdk/client-s3');

const { config } = require('../../../../../../lib/Config');
const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const {
    uniqName,
    gcpBucketMPU,
    gcpClient,
    gcpLocation,
    gcpLocationMismatch,
    memLocation,
    awsLocation,
    awsS3,
    getOwnerInfo,
    genUniqID,
    describeSkipIfNotMultiple,
} = require('../utils');

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
    Initiator: { ID: ownerID, DisplayName: ownerDisplayName },
    Owner: { DisplayName: ownerDisplayName, ID: ownerID },
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
    async.waterfall(
        [
            next => {
                s3.send(
                    new ListPartsCommand({
                        Bucket: bucketName,
                        Key: keyName,
                        UploadId: uploadId,
                    }),
                )
                    .then(res => {
                        resultCopy.Parts = [
                            {
                                PartNumber: 1,
                                LastModified: res.Parts[0].LastModified,
                                ETag: `"${md5}"`,
                                Size: totalSize,
                            },
                        ];
                        assert.deepStrictEqual(res, resultCopy);
                        next();
                    })
                    .catch(err => next(new Error(`listParts: Expected success, got error: ${err}`)));
            },
            next =>
                gcpClient.listParts(
                    {
                        Bucket: gcpBucketMPU,
                        Key: keyName,
                        UploadId: uploadId,
                    },
                    (err, res) => {
                        assert.ifError(err, 'GCP listParts: Expected success,' + `got error: ${err}`);
                        assert.strictEqual(res.Contents[0].ETag, `"${md5}"`);
                        next();
                    },
                ),
        ],
        cb,
    );
}

describeSkipIfNotMultiple('Put Copy Part to GCP', function describeFn() {
    this.timeout(800000);
    withV4(sigCfg => {
        beforeEach(done => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            s3.send(
                new CreateBucketCommand({
                    Bucket: bucket,
                    CreateBucketConfiguration: {
                        LocationConstraint: gcpLocation,
                    },
                }),
            )
                .then(() => done())
                .catch(done);
        });

        afterEach(() => {
            process.stdout.write('Emptying bucket\n');
            return bucketUtil
                .empty(bucket)
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
            beforeEach(function beforeFn(done) {
                this.currentTest.keyNameNormalGcp = `normalgcp${uniqName(keyObjectGcp)}`;
                this.currentTest.keyNameNormalGcpMismatch = `normalgcpmismatch${uniqName(keyObjectGcp)}`;

                this.currentTest.keyNameFiveMbGcp = `fivembgcp${uniqName(keyObjectGcp)}`;
                this.currentTest.keyNameFiveMbMem = `fivembmem${uniqName(keyObjectMemory)}`;

                this.currentTest.mpuKeyNameGcp = `mpukeyname${uniqName(keyObjectGcp)}`;
                this.currentTest.mpuKeyNameMem = `mpukeyname${uniqName(keyObjectMemory)}`;
                this.currentTest.mpuKeyNameAWS = `mpukeyname${uniqName(keyObjectAWS)}`;
                const paramsGcp = {
                    Bucket: bucket,
                    Key: this.currentTest.mpuKeyNameGcp,
                    Metadata: { 'scal-location-constraint': gcpLocation },
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
                async.waterfall(
                    [
                        next => {
                            s3.send(new CreateBucketCommand({ Bucket: bucket }))
                                .then(() => next())
                                .catch(next);
                        },
                        next => {
                            s3.send(new CreateBucketCommand({ Bucket: memBucketName }))
                                .then(() => next())
                                .catch(next);
                        },
                        next => {
                            s3.send(
                                new PutObjectCommand({
                                    Bucket: bucket,
                                    Key: this.currentTest.keyNameNormalGcp,
                                    Body: normalBody,
                                    Metadata: { 'scal-location-constraint': gcpLocation },
                                }),
                            )
                                .then(() => next())
                                .catch(next);
                        },
                        next => {
                            s3.send(
                                new PutObjectCommand({
                                    Bucket: bucket,
                                    Key: this.currentTest.keyNameNormalGcpMismatch,
                                    Body: normalBody,
                                    Metadata: { 'scal-location-constraint': gcpLocationMismatch },
                                }),
                            )
                                .then(() => next())
                                .catch(next);
                        },
                        next => {
                            s3.send(
                                new PutObjectCommand({
                                    Bucket: bucket,
                                    Key: this.currentTest.keyNameFiveMbGcp,
                                    Body: fiveMbBody,
                                    Metadata: { 'scal-location-constraint': gcpLocation },
                                }),
                            )
                                .then(() => next())
                                .catch(next);
                        },
                        next => {
                            s3.send(
                                new PutObjectCommand({
                                    Bucket: bucket,
                                    Key: this.currentTest.keyNameFiveMbMem,
                                    Body: fiveMbBody,
                                    Metadata: { 'scal-location-constraint': memLocation },
                                }),
                            )
                                .then(() => next())
                                .catch(next);
                        },
                        next => {
                            s3.send(new CreateMultipartUploadCommand(paramsGcp))
                                .then(res => {
                                    this.currentTest.uploadId = res.UploadId;
                                    next();
                                })
                                .catch(err =>
                                    next(
                                        new Error(`createMultipartUpload on gcp: Expected success, got error: ${err}`),
                                    ),
                                );
                        },
                        next => {
                            s3.send(new CreateMultipartUploadCommand(paramsMem))
                                .then(res => {
                                    this.currentTest.uploadIdMem = res.UploadId;
                                    next();
                                })
                                .catch(err =>
                                    next(
                                        new Error(
                                            `createMultipartUpload in memory: Expected success, got error: ${err}`,
                                        ),
                                    ),
                                );
                        },
                        next => {
                            s3.send(new CreateMultipartUploadCommand(paramsAWS))
                                .then(res => {
                                    this.currentTest.uploadIdAWS = res.UploadId;
                                    next();
                                })
                                .catch(err =>
                                    next(
                                        new Error(`createMultipartUpload on AWS: Expected success, got error: ${err}`),
                                    ),
                                );
                        },
                    ],
                    done,
                );
            });

            afterEach(function afterFn(done) {
                const paramsGcp = {
                    Bucket: bucket,
                    Key: this.currentTest.mpuKeyNameGcp,
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
                async.waterfall(
                    [
                        next => {
                            s3.send(new AbortMultipartUploadCommand(paramsGcp))
                                .then(() => next())
                                .catch(next);
                        },
                        next => {
                            s3.send(new AbortMultipartUploadCommand(paramsMem))
                                .then(() => next())
                                .catch(next);
                        },
                        next => {
                            s3.send(new AbortMultipartUploadCommand(paramsAWS))
                                .then(() => next())
                                .catch(next);
                        },
                    ],
                    done,
                );
            });

            it('should copy small part from GCP to MPU with GCP location', function itFn(done) {
                const params = {
                    Bucket: bucket,
                    CopySource: `${bucket}/${this.test.keyNameNormalGcp}`,
                    Key: this.test.mpuKeyNameGcp,
                    PartNumber: 1,
                    UploadId: this.test.uploadId,
                };
                async.waterfall(
                    [
                        next => {
                            s3.send(new UploadPartCopyCommand(params))
                                .then(res => {
                                    assert.strictEqual(res.ETag, `"${normalMD5}"`);
                                    next();
                                })
                                .catch(err => next(new Error(`uploadPartCopy: Expected success, got error: ${err}`)));
                        },
                        next => {
                            const infos = {
                                bucketName: bucket,
                                keyName: this.test.mpuKeyNameGcp,
                                uploadId: this.test.uploadId,
                                md5: normalMD5,
                                totalSize: normalBodySize,
                            };
                            assertCopyPart(infos, next);
                        },
                    ],
                    done,
                );
            });

            it(
                'should copy small part from GCP with bucketMatch=false to ' + 'MPU with GCP location',
                function itFn(done) {
                    const params = {
                        Bucket: bucket,
                        CopySource: `${bucket}/${this.test.keyNameNormalGcpMismatch}`,
                        Key: this.test.mpuKeyNameGcp,
                        PartNumber: 1,
                        UploadId: this.test.uploadId,
                    };
                    async.waterfall(
                        [
                            next => {
                                s3.send(new UploadPartCopyCommand(params))
                                    .then(res => {
                                        assert.strictEqual(res.ETag, `"${normalMD5}"`);
                                        next();
                                    })
                                    .catch(err =>
                                        next(new Error(`uploadPartCopy: Expected success, got error: ${err}`)),
                                    );
                            },
                            next => {
                                const infos = {
                                    bucketName: bucket,
                                    keyName: this.test.mpuKeyNameGcp,
                                    uploadId: this.test.uploadId,
                                    md5: normalMD5,
                                    totalSize: normalBodySize,
                                };
                                assertCopyPart(infos, next);
                            },
                        ],
                        done,
                    );
                },
            );

            it('should copy 5 Mb part from GCP to MPU with GCP location', function ifF(done) {
                const params = {
                    Bucket: bucket,
                    CopySource: `${bucket}/${this.test.keyNameFiveMbGcp}`,
                    Key: this.test.mpuKeyNameGcp,
                    PartNumber: 1,
                    UploadId: this.test.uploadId,
                };
                async.waterfall(
                    [
                        next => {
                            s3.send(new UploadPartCopyCommand(params))
                                .then(res => {
                                    assert.strictEqual(res.ETag, `"${fiveMbMD5}"`);
                                    next();
                                })
                                .catch(err => next(new Error(`uploadPartCopy: Expected success, got error: ${err}`)));
                        },
                        next => {
                            const infos = {
                                bucketName: bucket,
                                keyName: this.test.mpuKeyNameGcp,
                                uploadId: this.test.uploadId,
                                md5: fiveMbMD5,
                                totalSize: fiveMB,
                            };
                            assertCopyPart(infos, next);
                        },
                    ],
                    done,
                );
            });

            it('should copy part from GCP to MPU with memory location', function ifF(done) {
                const params = {
                    Bucket: memBucketName,
                    CopySource: `${bucket}/${this.test.keyNameNormalGcp}`,
                    Key: this.test.mpuKeyNameMem,
                    PartNumber: 1,
                    UploadId: this.test.uploadIdMem,
                };
                async.waterfall(
                    [
                        next => {
                            s3.send(new UploadPartCopyCommand(params))
                                .then(res => {
                                    assert.strictEqual(res.ETag, `"${normalMD5}"`);
                                    next();
                                })
                                .catch(err => next(new Error(`uploadPartCopy: Expected success, got error: ${err}`)));
                        },
                        next => {
                            s3.send(
                                new ListPartsCommand({
                                    Bucket: memBucketName,
                                    Key: this.test.mpuKeyNameMem,
                                    UploadId: this.test.uploadIdMem,
                                }),
                            )
                                .then(res => {
                                    const resultCopy = JSON.parse(JSON.stringify(result));
                                    resultCopy.Bucket = memBucketName;
                                    resultCopy.Key = this.test.mpuKeyNameMem;
                                    resultCopy.UploadId = this.test.uploadIdMem;
                                    resultCopy.Parts = [
                                        {
                                            PartNumber: 1,
                                            LastModified: res.Parts[0].LastModified,
                                            ETag: `"${normalMD5}"`,
                                            Size: normalBodySize,
                                        },
                                    ];
                                    assert.deepStrictEqual(res, resultCopy);
                                    next();
                                })
                                .catch(err => next(new Error(`listParts: Expected success, got error: ${err}`)));
                        },
                    ],
                    done,
                );
            });

            it('should copy part from GCP to MPU with AWS location', function ifF(done) {
                const params = {
                    Bucket: memBucketName,
                    CopySource: `${bucket}/${this.test.keyNameNormalGcp}`,
                    Key: this.test.mpuKeyNameAWS,
                    PartNumber: 1,
                    UploadId: this.test.uploadIdAWS,
                };
                async.waterfall(
                    [
                        next => {
                            s3.send(new UploadPartCopyCommand(params))
                                .then(res => {
                                    assert.strictEqual(res.ETag, `"${normalMD5}"`);
                                    next();
                                })
                                .catch(err => next(new Error(`uploadPartCopy: Expected success, got error: ${err}`)));
                        },
                        next => {
                            const awsBucket = config.locationConstraints[awsLocation].details.bucketName;
                            awsS3.listParts(
                                {
                                    Bucket: awsBucket,
                                    Key: this.test.mpuKeyNameAWS,
                                    UploadId: this.test.uploadIdAWS,
                                },
                                (err, res) => {
                                    assert.ifError(err, 'listParts: Expected success,' + ` got error: ${err}`);
                                    assert.strictEqual(res.Bucket, awsBucket);
                                    assert.strictEqual(res.Key, this.test.mpuKeyNameAWS);
                                    assert.strictEqual(res.UploadId, this.test.uploadIdAWS);
                                    assert.strictEqual(res.Parts.length, 1);
                                    assert.strictEqual(res.Parts[0].PartNumber, 1);
                                    assert.strictEqual(res.Parts[0].ETag, `"${normalMD5}"`);
                                    assert.strictEqual(res.Parts[0].Size, normalBodySize);
                                    next();
                                },
                            );
                        },
                    ],
                    done,
                );
            });

            it('should copy part from GCP object with range to MPU ' + 'with AWS location', function ifF(done) {
                const params = {
                    Bucket: memBucketName,
                    CopySource: `${bucket}/${this.test.keyNameNormalGcp}`,
                    Key: this.test.mpuKeyNameAWS,
                    CopySourceRange: 'bytes=0-5',
                    PartNumber: 1,
                    UploadId: this.test.uploadIdAWS,
                };
                async.waterfall(
                    [
                        next => {
                            s3.send(new UploadPartCopyCommand(params))
                                .then(res => {
                                    assert.strictEqual(res.ETag, `"${sixBytesMD5}"`);
                                    next();
                                })
                                .catch(err => next(new Error(`uploadPartCopy: Expected success, got error: ${err}`)));
                        },
                        next => {
                            const awsBucket = config.locationConstraints[awsLocation].details.bucketName;
                            awsS3.listParts(
                                {
                                    Bucket: awsBucket,
                                    Key: this.test.mpuKeyNameAWS,
                                    UploadId: this.test.uploadIdAWS,
                                },
                                (err, res) => {
                                    assert.ifError(err, 'listParts: Expected success,' + ` got error: ${err}`);
                                    assert.strictEqual(res.Bucket, awsBucket);
                                    assert.strictEqual(res.Key, this.test.mpuKeyNameAWS);
                                    assert.strictEqual(res.UploadId, this.test.uploadIdAWS);
                                    assert.strictEqual(res.Parts.length, 1);
                                    assert.strictEqual(res.Parts[0].PartNumber, 1);
                                    assert.strictEqual(res.Parts[0].ETag, `"${sixBytesMD5}"`);
                                    assert.strictEqual(res.Parts[0].Size, 6);
                                    next();
                                },
                            );
                        },
                    ],
                    done,
                );
            });

            it('should copy 5 Mb part from a memory location to MPU with ' + 'GCP location', function ifF(done) {
                const params = {
                    Bucket: bucket,
                    CopySource: `${bucket}/${this.test.keyNameFiveMbMem}`,
                    Key: this.test.mpuKeyNameGcp,
                    PartNumber: 1,
                    UploadId: this.test.uploadId,
                };
                async.waterfall(
                    [
                        next => {
                            s3.send(new UploadPartCopyCommand(params))
                                .then(res => {
                                    assert.strictEqual(res.ETag, `"${fiveMbMD5}"`);
                                    next();
                                })
                                .catch(err => next(new Error(`uploadPartCopy: Expected success, got error: ${err}`)));
                        },
                        next => {
                            const infos = {
                                bucketName: bucket,
                                keyName: this.test.mpuKeyNameGcp,
                                uploadId: this.test.uploadId,
                                md5: fiveMbMD5,
                                totalSize: fiveMB,
                            };
                            assertCopyPart(infos, next);
                        },
                    ],
                    done,
                );
            });

            describe('with existing part', () => {
                beforeEach(function beF(done) {
                    const params = {
                        Body: oneKbBody,
                        Bucket: bucket,
                        Key: this.currentTest.mpuKeyNameGcp,
                        PartNumber: 1,
                        UploadId: this.currentTest.uploadId,
                    };
                    s3.send(new UploadPartCommand(params))
                        .then(() => done())
                        .catch(done);
                });
                it('should copy part from GCP to GCP with existing ' + 'parts', function ifF(done) {
                    const resultCopy = JSON.parse(JSON.stringify(result));
                    const params = {
                        Bucket: bucket,
                        CopySource: `${bucket}/${this.test.keyNameNormalGcp}`,
                        Key: this.test.mpuKeyNameGcp,
                        PartNumber: 2,
                        UploadId: this.test.uploadId,
                    };
                    async.waterfall(
                        [
                            next => {
                                s3.send(new UploadPartCopyCommand(params))
                                    .then(res => {
                                        assert.strictEqual(res.ETag, `"${normalMD5}"`);
                                        next();
                                    })
                                    .catch(err =>
                                        next(new Error(`uploadPartCopy: Expected success, got error: ${err}`)),
                                    );
                            },
                            next => {
                                s3.send(
                                    new ListPartsCommand({
                                        Bucket: bucket,
                                        Key: this.test.mpuKeyNameGcp,
                                        UploadId: this.test.uploadId,
                                    }),
                                )
                                    .then(res => {
                                        resultCopy.Bucket = bucket;
                                        resultCopy.Key = this.test.mpuKeyNameGcp;
                                        resultCopy.UploadId = this.test.uploadId;
                                        resultCopy.Parts = [
                                            {
                                                PartNumber: 1,
                                                LastModified: res.Parts[0].LastModified,
                                                ETag: `"${oneKbMD5}"`,
                                                Size: oneKb,
                                            },
                                            {
                                                PartNumber: 2,
                                                LastModified: res.Parts[1].LastModified,
                                                ETag: `"${normalMD5}"`,
                                                Size: 11,
                                            },
                                        ];
                                        assert.deepStrictEqual(res, resultCopy);
                                        next();
                                    })
                                    .catch(err => next(new Error(`listParts: Expected success, got error: ${err}`)));
                            },
                            next =>
                                gcpClient.listParts(
                                    {
                                        Bucket: gcpBucketMPU,
                                        Key: this.test.mpuKeyNameGcp,
                                        UploadId: this.test.uploadId,
                                    },
                                    (err, res) => {
                                        assert.ifError(err, 'GCP listParts: Expected ' + `success, got error: ${err}`);
                                        assert.strictEqual(res.Contents[0].ETag, `"${oneKbMD5}"`);
                                        assert.strictEqual(res.Contents[1].ETag, `"${normalMD5}"`);
                                        next();
                                    },
                                ),
                        ],
                        done,
                    );
                });
            });
        });
    });
});

describeSkipIfNotMultiple('Put Copy Part to GCP with complete MPU', function describeF() {
    this.timeout(800000);
    withV4(sigCfg => {
        beforeEach(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
        });

        afterEach(() => {
            process.stdout.write('Emptying bucket\n');
            return bucketUtil
                .empty(bucket)
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
        describe('Basic test with complete MPU from AWS to GCP location: ', () => {
            beforeEach(function beF(done) {
                this.currentTest.keyNameAws = `onehundredandfivembgcp${uniqName(keyObjectAWS)}`;
                this.currentTest.mpuKeyNameGcp = `mpukeyname${uniqName(keyObjectGcp)}`;

                const createMpuParams = {
                    Bucket: bucket,
                    Key: this.currentTest.mpuKeyNameGcp,
                    Metadata: { 'scal-location-constraint': gcpLocation },
                };
                async.waterfall(
                    [
                        next => {
                            s3.send(new CreateBucketCommand({ Bucket: awsBucketName }))
                                .then(() => next())
                                .catch(next);
                        },
                        next => {
                            s3.send(new CreateBucketCommand({ Bucket: bucket }))
                                .then(() => next())
                                .catch(next);
                        },
                        next => {
                            s3.send(
                                new PutObjectCommand({
                                    Bucket: awsBucketName,
                                    Key: this.currentTest.keyNameAws,
                                    Body: fiveMbBody,
                                    Metadata: { 'scal-location-constraint': awsLocation },
                                }),
                            )
                                .then(() => next())
                                .catch(next);
                        },
                        next => {
                            s3.send(new CreateMultipartUploadCommand(createMpuParams))
                                .then(res => {
                                    this.currentTest.uploadId = res.UploadId;
                                    next();
                                })
                                .catch(err =>
                                    next(new Error(`createMultipartUpload: Expected success, got error: ${err}`)),
                                );
                        },
                    ],
                    done,
                );
            });

            it('should copy two 5 MB part from GCP to MPU with GCP' + 'location', function ifF(done) {
                const uploadParams = {
                    Bucket: bucket,
                    CopySource: `${awsBucketName}/` + `${this.test.keyNameAws}`,
                    Key: this.test.mpuKeyNameGcp,
                    PartNumber: 1,
                    UploadId: this.test.uploadId,
                };
                const uploadParams2 = {
                    Bucket: bucket,
                    CopySource: `${awsBucketName}/` + `${this.test.keyNameAws}`,
                    Key: this.test.mpuKeyNameGcp,
                    PartNumber: 2,
                    UploadId: this.test.uploadId,
                };
                async.waterfall(
                    [
                        next => {
                            s3.send(new UploadPartCopyCommand(uploadParams))
                                .then(res => {
                                    assert.strictEqual(res.ETag, `"${fiveMbMD5}"`);
                                    next();
                                })
                                .catch(err => next(new Error(`uploadPartCopy: Expected success, got error: ${err}`)));
                        },
                        next => {
                            s3.send(new UploadPartCopyCommand(uploadParams2))
                                .then(res => {
                                    assert.strictEqual(res.ETag, `"${fiveMbMD5}"`);
                                    next();
                                })
                                .catch(err => next(new Error(`uploadPartCopy: Expected success, got error: ${err}`)));
                        },
                        next => {
                            const completeMpuParams = {
                                Bucket: bucket,
                                Key: this.test.mpuKeyNameGcp,
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
                                UploadId: this.test.uploadId,
                            };
                            s3.send(new CompleteMultipartUploadCommand(completeMpuParams))
                                .then(res => {
                                    assert.strictEqual(res.Bucket, bucket);
                                    assert.strictEqual(res.Key, this.test.mpuKeyNameGcp);
                                    next();
                                })
                                .catch(err =>
                                    next(new Error(`completeMultipartUpload: Expected success, got error: ${err}`)),
                                );
                        },
                    ],
                    done,
                );
            });
        });
    });
});
