const assert = require('assert');
const async = require('async');
const {
    CreateBucketCommand,
    DeleteBucketCommand,
    CreateMultipartUploadCommand,
    UploadPartCommand,
    AbortMultipartUploadCommand,
    PutObjectCommand,
    DeleteObjectCommand,
    GetObjectCommand,
} = require('@aws-sdk/client-s3');

const { s3middleware } = require('arsenal');
const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const {
    expectedETag,
    uniqName,
    getAzureClient,
    getAzureContainerName,
    convertMD5,
    azureLocation,
    azureLocationMismatch,
    describeSkipIfNotMultiple,
} = require('../utils');
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
    azureClient
        .getContainerClient(azureContainerName)
        .getBlockBlobClient(key)
        .getBlockList('all')
        .then(list => {
            const uncommittedBlocks = list.uncommittedBlocks;
            const committedBlocks = list.committedBlocks;
            assert.strictEqual(committedBlocks, undefined);
            uncommittedBlocks.forEach((l, index) => {
                assert.strictEqual(
                    l.name,
                    getBlockId(uploadId, expectedParts[index].partnbr, expectedParts[index].subpartnbr),
                );
                assert.strictEqual(l.size, expectedParts[index].size.toString());
            });
            cb();
        }, cb);
}

function azureCheck(key, cb) {
    (async () => {
        const res = await s3.send(
            new GetObjectCommand({
                Bucket: azureContainerName,
                Key: key,
            }),
        );
        assert(res, 'expected getObject response');
        assert.strictEqual(res.ETag, `"${expectedMD5}"`);
        const properties = await azureClient.getContainerClient(azureContainerName).getProperties(key);
        const convertedMD5 = convertMD5(properties.contentSettings.contentMD5);
        assert.strictEqual(convertedMD5, expectedMD5);
    })()
        .then(() => cb())
        .catch(err => cb(err));
}

describeSkipIfNotMultiple('MultipleBackend put part to AZURE', function describeF() {
    this.timeout(80000);
    withV4(sigCfg => {
        beforeEach(function beforeFn() {
            this.currentTest.key = uniqName(keyObject);
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
        });
        describe('with bucket location header', () => {
            beforeEach(function beforeEachFn(done) {
                async.waterfall(
                    [
                        next => {
                            s3.send(
                                new CreateBucketCommand({
                                    Bucket: azureContainerName,
                                }),
                            )
                                .then(() => next())
                                .catch(next);
                        },
                        next => {
                            s3.send(
                                new CreateMultipartUploadCommand({
                                    Bucket: azureContainerName,
                                    Key: this.currentTest.key,
                                    Metadata: { 'scal-location-constraint': azureLocation },
                                }),
                            )
                                .then(res => {
                                    this.currentTest.uploadId = res.UploadId;
                                    next();
                                })
                                .catch(next);
                        },
                    ],
                    done,
                );
            });

            afterEach(function afterEachFn(done) {
                async.waterfall(
                    [
                        next => {
                            s3.send(
                                new AbortMultipartUploadCommand({
                                    Bucket: azureContainerName,
                                    Key: this.currentTest.key,
                                    UploadId: this.currentTest.uploadId,
                                }),
                            )
                                .then(() => next())
                                .catch(next);
                        },
                        next => {
                            s3.send(
                                new DeleteBucketCommand({
                                    Bucket: azureContainerName,
                                }),
                            )
                                .then(() => next())
                                .catch(next);
                        },
                    ],
                    err => {
                        assert.equal(err, null, `Error aborting MPU: ${err}`);
                        done();
                    },
                );
            });

            it('should put 0-byte block to Azure', function itFn(done) {
                const params = {
                    Bucket: azureContainerName,
                    Key: this.test.key,
                    UploadId: this.test.uploadId,
                    PartNumber: 1,
                };
                async.waterfall(
                    [
                        next => {
                            const uploadParams = {
                                ...params,
                                Body: Buffer.alloc(0),
                            };
                            s3.send(new UploadPartCommand(uploadParams))
                                .then(res => {
                                    const eTagExpected = `"${azureMpuUtils.zeroByteETag}"`;
                                    assert.strictEqual(res.ETag, eTagExpected);
                                    next();
                                })
                                .catch(next);
                        },
                        next =>
                            azureClient
                                .getContainerClient(azureContainerName)
                                .getBlockBlobClient(this.test.key)
                                .getBlockList('all')
                                .then(
                                    () => assert.fail('Expected failure but got success'),
                                    err => {
                                        assert.strictEqual(err.code, 'BlobNotFound');
                                        next();
                                    },
                                ),
                    ],
                    done,
                );
            });

            it('should put 2 blocks to Azure', function itFn(done) {
                const body = Buffer.alloc(maxSubPartSize + 10);
                const parts = [
                    { partnbr: 1, subpartnbr: 0, size: maxSubPartSize },
                    { partnbr: 1, subpartnbr: 1, size: 10 },
                ];
                const params = {
                    Bucket: azureContainerName,
                    Key: this.test.key,
                    UploadId: this.test.uploadId,
                    PartNumber: 1,
                    Body: body,
                };
                async.waterfall(
                    [
                        next => {
                            s3.send(new UploadPartCommand(params))
                                .then(res => {
                                    const eTagExpected = expectedETag(body);
                                    assert.strictEqual(res.ETag, eTagExpected);
                                    next();
                                })
                                .catch(next);
                        },
                        next => checkSubPart(this.test.key, this.test.uploadId, parts, next),
                    ],
                    done,
                );
            });

            it('should put 5 parts bigger than maxSubPartSize to Azure', function it(done) {
                const body = Buffer.alloc(maxSubPartSize + 10);
                let parts = [];
                for (let i = 1; i < 6; i++) {
                    parts = parts.concat([
                        { partnbr: i, subpartnbr: 0, size: maxSubPartSize },
                        { partnbr: i, subpartnbr: 1, size: 10 },
                    ]);
                }
                async.times(
                    5,
                    (n, next) => {
                        const partNumber = n + 1;
                        const params = {
                            Bucket: azureContainerName,
                            Key: this.test.key,
                            UploadId: this.test.uploadId,
                            PartNumber: partNumber,
                            Body: body,
                        };
                        s3.send(new UploadPartCommand(params))
                            .then(res => {
                                const eTagExpected = expectedETag(body);
                                assert.strictEqual(res.ETag, eTagExpected);
                                next();
                            })
                            .catch(next);
                    },
                    err => {
                        assert.equal(err, null, 'Expected success, ' + `got error: ${err}`);
                        checkSubPart(this.test.key, this.test.uploadId, parts, done);
                    },
                );
            });

            it('should put 5 parts smaller than maxSubPartSize to Azure', function it(done) {
                const body = Buffer.alloc(10);
                let parts = [];
                for (let i = 1; i < 6; i++) {
                    parts = parts.concat([{ partnbr: i, subpartnbr: 0, size: 10 }]);
                }
                async.times(
                    5,
                    (n, next) => {
                        const partNumber = n + 1;
                        const params = {
                            Bucket: azureContainerName,
                            Key: this.test.key,
                            UploadId: this.test.uploadId,
                            PartNumber: partNumber,
                            Body: body,
                        };
                        s3.send(new UploadPartCommand(params))
                            .then(res => {
                                const eTagExpected = expectedETag(body);
                                assert.strictEqual(res.ETag, eTagExpected);
                                next();
                            })
                            .catch(next);
                    },
                    err => {
                        assert.equal(err, null, 'Expected success, ' + `got error: ${err}`);
                        checkSubPart(this.test.key, this.test.uploadId, parts, done);
                    },
                );
            });

            it('should put the same part twice', function itFn(done) {
                const body1 = Buffer.alloc(maxSubPartSize + 10);
                const body2 = Buffer.alloc(20);
                const parts2 = [
                    { partnbr: 1, subpartnbr: 0, size: 20 },
                    { partnbr: 1, subpartnbr: 1, size: 10 },
                ];
                async.waterfall(
                    [
                        next => {
                            s3.send(
                                new UploadPartCommand({
                                    Bucket: azureContainerName,
                                    Key: this.test.key,
                                    UploadId: this.test.uploadId,
                                    PartNumber: 1,
                                    Body: body1,
                                }),
                            )
                                .then(() => next())
                                .catch(next);
                        },
                        next => {
                            s3.send(
                                new UploadPartCommand({
                                    Bucket: azureContainerName,
                                    Key: this.test.key,
                                    UploadId: this.test.uploadId,
                                    PartNumber: 1,
                                    Body: body2,
                                }),
                            )
                                .then(res => {
                                    const eTagExpected = expectedETag(body2);
                                    assert.strictEqual(res.ETag, eTagExpected);
                                    next();
                                })
                                .catch(next);
                        },
                        next => checkSubPart(this.test.key, this.test.uploadId, parts2, next),
                    ],
                    done,
                );
            });
        });

        describe('with same key as preexisting part', () => {
            beforeEach(function beforeEachFn(done) {
                async.waterfall(
                    [
                        next => {
                            s3.send(
                                new CreateBucketCommand({
                                    Bucket: azureContainerName,
                                }),
                            )
                                .then(() => next())
                                .catch(next);
                        },
                        next => {
                            const body = Buffer.alloc(10);
                            s3.send(
                                new PutObjectCommand({
                                    Bucket: azureContainerName,
                                    Key: this.currentTest.key,
                                    Metadata: { 'scal-location-constraint': azureLocation },
                                    Body: body,
                                }),
                            )
                                .then(() => next())
                                .catch(err => {
                                    assert.equal(err, null, 'Err putting object to ' + `azure: ${err}`);
                                    next(err);
                                });
                        },
                        next => {
                            s3.send(
                                new CreateMultipartUploadCommand({
                                    Bucket: azureContainerName,
                                    Key: this.currentTest.key,
                                    Metadata: { 'scal-location-constraint': azureLocation },
                                }),
                            )
                                .then(res => {
                                    this.currentTest.uploadId = res.UploadId;
                                    next();
                                })
                                .catch(next);
                        },
                    ],
                    done,
                );
            });

            afterEach(function afterEachFn(done) {
                async.waterfall(
                    [
                        next => {
                            process.stdout.write('Aborting multipart upload\n');
                            s3.send(
                                new AbortMultipartUploadCommand({
                                    Bucket: azureContainerName,
                                    Key: this.currentTest.key,
                                    UploadId: this.currentTest.uploadId,
                                }),
                            )
                                .then(() => next())
                                .catch(next);
                        },
                        next => {
                            process.stdout.write('Deleting object\n');
                            s3.send(
                                new DeleteObjectCommand({
                                    Bucket: azureContainerName,
                                    Key: this.currentTest.key,
                                }),
                            )
                                .then(() => next())
                                .catch(next);
                        },
                        next => {
                            process.stdout.write('Deleting bucket\n');
                            s3.send(
                                new DeleteBucketCommand({
                                    Bucket: azureContainerName,
                                }),
                            )
                                .then(() => next())
                                .catch(next);
                        },
                    ],
                    err => {
                        assert.equal(err, null, `Err in afterEach: ${err}`);
                        done();
                    },
                );
            });

            it('should put a part without overwriting existing object', function itFn(done) {
                const body = Buffer.alloc(20);
                s3.send(
                    new UploadPartCommand({
                        Bucket: azureContainerName,
                        Key: this.test.key,
                        UploadId: this.test.uploadId,
                        PartNumber: 1,
                        Body: body,
                    }),
                )
                    .then(() => {
                        azureCheck(this.test.key, done);
                    })
                    .catch(err => {
                        assert.strictEqual(err, null, 'Err putting part to ' + `Azure: ${err}`);
                        done(err);
                    });
            });
        });
    });
});

describeSkipIfNotMultiple(
    'MultipleBackend put part to AZURE ' + 'location with bucketMatch sets to false',
    function describeF() {
        this.timeout(80000);
        withV4(sigCfg => {
            beforeEach(function beforeFn() {
                this.currentTest.key = uniqName(keyObject);
                bucketUtil = new BucketUtility('default', sigCfg);
                s3 = bucketUtil.s3;
            });
            describe('with bucket location header', () => {
                beforeEach(function beforeEachFn(done) {
                    async.waterfall(
                        [
                            next => {
                                s3.send(
                                    new CreateBucketCommand({
                                        Bucket: azureContainerName,
                                    }),
                                )
                                    .then(() => next())
                                    .catch(next);
                            },
                            next => {
                                s3.send(
                                    new CreateMultipartUploadCommand({
                                        Bucket: azureContainerName,
                                        Key: this.currentTest.key,
                                        Metadata: { 'scal-location-constraint': azureLocationMismatch },
                                    }),
                                )
                                    .then(res => {
                                        this.currentTest.uploadId = res.UploadId;
                                        next();
                                    })
                                    .catch(next);
                            },
                        ],
                        done,
                    );
                });

                afterEach(function afterEachFn(done) {
                    async.waterfall(
                        [
                            next => {
                                s3.send(
                                    new AbortMultipartUploadCommand({
                                        Bucket: azureContainerName,
                                        Key: this.currentTest.key,
                                        UploadId: this.currentTest.uploadId,
                                    }),
                                )
                                    .then(() => next())
                                    .catch(next);
                            },
                            next => {
                                s3.send(
                                    new DeleteBucketCommand({
                                        Bucket: azureContainerName,
                                    }),
                                )
                                    .then(() => next())
                                    .catch(next);
                            },
                        ],
                        err => {
                            assert.equal(err, null, `Error aborting MPU: ${err}`);
                            done();
                        },
                    );
                });

                it('should put block to AZURE location with bucketMatch' + ' sets to false', function itFn(done) {
                    const body20 = Buffer.alloc(20);
                    const params = {
                        Bucket: azureContainerName,
                        Key: this.test.key,
                        UploadId: this.test.uploadId,
                        PartNumber: 1,
                        Body: body20,
                    };
                    const parts = [{ partnbr: 1, subpartnbr: 0, size: 20 }];
                    async.waterfall(
                        [
                            next => {
                                s3.send(new UploadPartCommand(params))
                                    .then(res => {
                                        const eTagExpected = '"441018525208457705bf09a8ee3c1093"';
                                        assert.strictEqual(res.ETag, eTagExpected);
                                        next();
                                    })
                                    .catch(next);
                            },
                            next =>
                                checkSubPart(`${azureContainerName}/${this.test.key}`, this.test.uploadId, parts, next),
                        ],
                        done,
                    );
                });
            });
        });
    },
);
