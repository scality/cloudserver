const assert = require('assert');
const crypto = require('crypto');

const {
    CreateBucketCommand,
    PutObjectCommand,
    GetObjectCommand,
    HeadObjectCommand,
    CreateMultipartUploadCommand,
    UploadPartCommand,
    UploadPartCopyCommand,
    CompleteMultipartUploadCommand,
    AbortMultipartUploadCommand,
    PutObjectAclCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const { createEncryptedBucketPromise } = require('../../lib/utility/createEncryptedBucket');
const { fakeMetadataTransition, fakeMetadataArchive } = require('../utils/init');
const { hasColdStorage } = require('../../lib/utility/test-utils');

const sourceBucketName = 'supersourcebucket81033016532';
const sourceObjName = 'supersourceobject';
const destBucketName = 'destinationbucket815502017';
const destObjName = 'copycatobject';
const content = 'I am the best content ever';

const otherAccountBucketUtility = new BucketUtility('lisa', {});
const otherAccountS3 = otherAccountBucketUtility.s3;

// in constants, we set 110 MB as the max part size for testing purposes
const oneHundredMBPlus11 = 110100481;

function checkNoError(err) {
    assert.equal(err, null, `Expected success, got error ${JSON.stringify(err)}`);
}

function checkError(err, code) {
    assert.notEqual(err, null, 'Expected failure but got success');
    assert.strictEqual(err.name, code);
}

describe('Object Part Copy', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;
        let etag;
        let uploadId;

        beforeEach(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            s3.createBucketPromise = params => s3.send(new CreateBucketCommand(params));
            if (process.env.ENABLE_KMS_ENCRYPTION === 'true') {
                s3.createBucketPromise = createEncryptedBucketPromise;
            }
            return s3
                .createBucketPromise({ Bucket: sourceBucketName })
                .catch(err => {
                    process.stdout.write(`Error creating source bucket: ${err}\n`);
                    throw err;
                })
                .then(() => s3.createBucketPromise({ Bucket: destBucketName }))
                .catch(err => {
                    process.stdout.write(`Error creating dest bucket: ${err}\n`);
                    throw err;
                })
                .then(() =>
                    s3.send(
                        new PutObjectCommand({
                            Bucket: sourceBucketName,
                            Key: sourceObjName,
                            Body: content,
                        }),
                    ),
                )
                .then(res => {
                    etag = res.ETag;
                    return s3.send(
                        new HeadObjectCommand({
                            Bucket: sourceBucketName,
                            Key: sourceObjName,
                        }),
                    );
                })
                .then(() =>
                    s3
                        .send(
                            new CreateMultipartUploadCommand({
                                Bucket: destBucketName,
                                Key: destObjName,
                            }),
                        )
                        .then(initiateRes => {
                            uploadId = initiateRes.UploadId;
                        }),
                )
                .catch(err => {
                    process.stdout.write(`Error in outer beforeEach: ${err}\n`);
                    throw err;
                });
        });

        afterEach(() =>
            bucketUtil
                .empty(sourceBucketName)
                .then(() => bucketUtil.empty(destBucketName))
                .then(() =>
                    s3.send(
                        new AbortMultipartUploadCommand({
                            Bucket: destBucketName,
                            Key: destObjName,
                            UploadId: uploadId,
                        }),
                    ),
                )
                .catch(err => {
                    if (err.name !== 'NoSuchUpload') {
                        process.stdout.write(`Error in afterEach: ${err}\n`);
                        throw err;
                    }
                })
                .then(() => bucketUtil.deleteMany([sourceBucketName, destBucketName])),
        );

        it('should copy a part from a source bucket to a different ' + 'destination bucket', () =>
            s3
                .send(
                    new UploadPartCopyCommand({
                        Bucket: destBucketName,
                        Key: destObjName,
                        CopySource: `${sourceBucketName}/${sourceObjName}`,
                        PartNumber: 1,
                        UploadId: uploadId,
                    }),
                )
                .then(res => {
                    assert.strictEqual(res.CopyPartResult.ETag, etag);
                    assert(res.CopyPartResult.LastModified);
                }),
        );

        it('should copy a part from a source bucket to a different ' + 'destination bucket and complete the MPU', () =>
            s3
                .send(
                    new UploadPartCopyCommand({
                        Bucket: destBucketName,
                        Key: destObjName,
                        CopySource: `${sourceBucketName}/${sourceObjName}`,
                        PartNumber: 1,
                        UploadId: uploadId,
                    }),
                )
                .then(res => {
                    assert.strictEqual(res.CopyPartResult.ETag, etag);
                    assert(res.CopyPartResult.LastModified);
                    return s3
                        .send(
                            new CompleteMultipartUploadCommand({
                                Bucket: destBucketName,
                                Key: destObjName,
                                UploadId: uploadId,
                                MultipartUpload: {
                                    Parts: [{ ETag: etag, PartNumber: 1 }],
                                },
                            }),
                        )
                        .then(res => {
                            assert.strictEqual(res.Bucket, destBucketName);
                            assert.strictEqual(res.Key, destObjName);
                            // AWS confirmed final ETag for MPU
                            assert.strictEqual(res.ETag, '"db77ebbae9e9f5a244a26b86193ad818-1"');
                        });
                }),
        );

        it('should return InvalidArgument error given invalid range', () =>
            s3
                .send(
                    new PutObjectCommand({
                        Bucket: sourceBucketName,
                        Key: sourceObjName,
                        Body: Buffer.alloc(oneHundredMBPlus11, 'packing'),
                    }),
                )
                .then(() =>
                    s3
                        .send(
                            new UploadPartCopyCommand({
                                Bucket: destBucketName,
                                Key: destObjName,
                                CopySource: `${sourceBucketName}/${sourceObjName}`,
                                PartNumber: 1,
                                UploadId: uploadId,
                                CopySourceRange: 'bad-range-parameter',
                            }),
                        )
                        .catch(err => {
                            checkError(err, 'InvalidArgument');
                        }),
                ));

        it(
            'should return EntityTooLarge error if attempt to copy ' +
                'object larger than max and do not specify smaller ' +
                'range in request',
            () =>
                s3
                    .send(
                        new PutObjectCommand({
                            Bucket: sourceBucketName,
                            Key: sourceObjName,
                            Body: Buffer.alloc(oneHundredMBPlus11, 'packing'),
                        }),
                    )
                    .then(() =>
                        s3.send(
                            new UploadPartCopyCommand({
                                Bucket: destBucketName,
                                Key: destObjName,
                                CopySource: `${sourceBucketName}/${sourceObjName}`,
                                PartNumber: 1,
                                UploadId: uploadId,
                            }),
                        ),
                    )
                    .catch(err => {
                        checkError(err, 'EntityTooLarge');
                    }),
        );

        it(
            'should return EntityTooLarge error if attempt to copy ' +
                'object larger than max and specify too large ' +
                'range in request',
            () =>
                s3
                    .send(
                        new PutObjectCommand({
                            Bucket: sourceBucketName,
                            Key: sourceObjName,
                            Body: Buffer.alloc(oneHundredMBPlus11, 'packing'),
                        }),
                    )
                    .then(() =>
                        s3.send(
                            new UploadPartCopyCommand({
                                Bucket: destBucketName,
                                Key: destObjName,
                                CopySource: `${sourceBucketName}/${sourceObjName}`,
                                PartNumber: 1,
                                UploadId: uploadId,
                                CopySourceRange: `bytes=0-${oneHundredMBPlus11}`,
                            }),
                        ),
                    )
                    .catch(err => {
                        checkError(err, 'EntityTooLarge');
                    }),
        );

        it(
            'should succeed if attempt to copy ' +
                'object larger than max but specify acceptable ' +
                'range in request',
            () =>
                s3
                    .send(
                        new PutObjectCommand({
                            Bucket: sourceBucketName,
                            Key: sourceObjName,
                            Body: Buffer.alloc(oneHundredMBPlus11, 'packing'),
                        }),
                    )
                    .then(() =>
                        s3.send(
                            new UploadPartCopyCommand({
                                Bucket: destBucketName,
                                Key: destObjName,
                                CopySource: `${sourceBucketName}/${sourceObjName}`,
                                PartNumber: 1,
                                UploadId: uploadId,
                                CopySourceRange: 'bytes=0-100',
                            }),
                        ),
                    )
                    .catch(err => {
                        checkNoError(err);
                    }),
        );

        it(
            'should copy a 0 byte object part from a source bucket to a ' +
                'different destination bucket and complete the MPU',
            () => {
                const emptyFileETag = '"d41d8cd98f00b204e9800998ecf8427e"';
                return s3
                    .send(
                        new PutObjectCommand({
                            Bucket: sourceBucketName,
                            Key: sourceObjName,
                            Body: '',
                        }),
                    )
                    .then(() =>
                        s3
                            .send(
                                new UploadPartCopyCommand({
                                    Bucket: destBucketName,
                                    Key: destObjName,
                                    CopySource: `${sourceBucketName}/${sourceObjName}`,
                                    PartNumber: 1,
                                    UploadId: uploadId,
                                }),
                            )
                            .then(res => {
                                assert.strictEqual(res.CopyPartResult.ETag, emptyFileETag);
                                assert(res.CopyPartResult.LastModified);
                                return s3
                                    .send(
                                        new CompleteMultipartUploadCommand({
                                            Bucket: destBucketName,
                                            Key: destObjName,
                                            UploadId: uploadId,
                                            MultipartUpload: {
                                                Parts: [{ ETag: emptyFileETag, PartNumber: 1 }],
                                            },
                                        }),
                                    )
                                    .then(res => {
                                        assert.strictEqual(res.Bucket, destBucketName);
                                        assert.strictEqual(res.Key, destObjName);
                                        // AWS confirmed final ETag for MPU
                                        assert.strictEqual(res.ETag, '"59adb24ef3cdbe0297f05b395827453f-1"');
                                    });
                            }),
                    );
            },
        );

        it(
            'should copy a part using a range header from a source bucket ' +
                'to a different destination bucket and complete the MPU',
            () => {
                const rangeETag = '"ac1be00f1f162e20d58099eec2ea1c70"';
                // AWS confirmed final ETag for MPU
                const finalMpuETag = '"bff2a6af3adfd8e107a06de01d487176-1"';
                return s3
                    .send(
                        new UploadPartCopyCommand({
                            Bucket: destBucketName,
                            Key: destObjName,
                            CopySource: `${sourceBucketName}/${sourceObjName}`,
                            PartNumber: 1,
                            CopySourceRange: 'bytes=0-3',
                            UploadId: uploadId,
                        }),
                    )
                    .then(res => {
                        assert.strictEqual(res.CopyPartResult.ETag, rangeETag);
                        assert(res.CopyPartResult.LastModified);
                        return s3
                            .send(
                                new CompleteMultipartUploadCommand({
                                    Bucket: destBucketName,
                                    Key: destObjName,
                                    UploadId: uploadId,
                                    MultipartUpload: {
                                        Parts: [{ ETag: rangeETag, PartNumber: 1 }],
                                    },
                                }),
                            )
                            .then(res => {
                                assert.strictEqual(res.Bucket, destBucketName);
                                assert.strictEqual(res.Key, destObjName);
                                assert.strictEqual(res.ETag, finalMpuETag);
                                return s3
                                    .send(
                                        new GetObjectCommand({
                                            Bucket: destBucketName,
                                            Key: destObjName,
                                        }),
                                    )
                                    .then(async res => {
                                        assert.strictEqual(res.ETag, finalMpuETag);
                                        assert.strictEqual(res.ContentLength, 4);
                                        const body = await res.Body.transformToString();
                                        assert.strictEqual(body, 'I am');
                                    });
                            });
                    });
            },
        );

        describe('When copy source was put by MPU', () => {
            let sourceMpuId;
            const sourceMpuKey = 'sourceMpuKey';
            // total hash for sourceMpuKey when MPU completed
            // (confirmed with AWS)
            const totalMpuObjectHash = '"9b0de95bd76728c778b9e25fd7ce2ef7"';

            beforeEach(() => {
                const parts = [];
                const md5HashPart = crypto.createHash('md5');
                const partBuff = Buffer.alloc(5242880);
                md5HashPart.update(partBuff);
                const partHash = md5HashPart.digest('hex');
                const otherMd5HashPart = crypto.createHash('md5');
                const otherPartBuff = Buffer.alloc(5242880, 1);
                otherMd5HashPart.update(otherPartBuff);
                const otherPartHash = otherMd5HashPart.digest('hex');
                return s3
                    .send(
                        new CreateMultipartUploadCommand({
                            Bucket: sourceBucketName,
                            Key: sourceMpuKey,
                        }),
                    )
                    .then(initiateRes => {
                        sourceMpuId = initiateRes.UploadId;
                    })
                    .catch(err => {
                        process.stdout.write(`Error initiating MPU in MPU beforeEach: ${err}\n`);
                        throw err;
                    })
                    .then(() => {
                        const partUploads = [];
                        // Concurrent uploads help trigger flakiness with "TimeoutError:
                        // socket hang up" due to a keep-alive race: the server closes
                        // an idle connection just as the client picks it from the pool.
                        const uploadWithRetry = (params, attempt = 0) =>
                            s3.send(new UploadPartCommand(params)).catch(err => {
                                if (attempt < 3) {
                                    process.stdout.write(
                                        `Retrying UploadPart ${params.PartNumber} ` +
                                            `(attempt ${attempt + 1}/3): ${err}\n`,
                                    );
                                    return uploadWithRetry(params, attempt + 1);
                                }
                                throw err;
                            });
                        for (let i = 1; i < 10; i++) {
                            const partBuffHere = i % 2 ? partBuff : otherPartBuff;
                            const partHashHere = i % 2 ? partHash : otherPartHash;
                            partUploads.push(
                                uploadWithRetry({
                                    Bucket: sourceBucketName,
                                    Key: sourceMpuKey,
                                    PartNumber: i,
                                    UploadId: sourceMpuId,
                                    Body: partBuffHere,
                                }),
                            );
                            parts.push({
                                ETag: partHashHere,
                                PartNumber: i,
                            });
                        }
                        process.stdout.write('about to put parts\n');
                        return Promise.all(partUploads);
                    })
                    .catch(err => {
                        process.stdout.write(`Error putting parts in MPU beforeEach: ${err}\n`);
                        throw err;
                    })
                    .then(() => {
                        process.stdout.write('completing mpu\n');
                        return s3.send(
                            new CompleteMultipartUploadCommand({
                                Bucket: sourceBucketName,
                                Key: sourceMpuKey,
                                UploadId: sourceMpuId,
                                MultipartUpload: {
                                    Parts: parts,
                                },
                            }),
                        );
                    })
                    .then(() => {
                        process.stdout.write('finished completing mpu\n');
                    })
                    .catch(err => {
                        process.stdout.write(`Error in MPU beforeEach: ${err}\n`);
                        throw err;
                    });
            });

            afterEach(() =>
                s3
                    .send(
                        new AbortMultipartUploadCommand({
                            Bucket: sourceBucketName,
                            Key: sourceMpuKey,
                            UploadId: sourceMpuId,
                        }),
                    )
                    .catch(err => {
                        if (err.name !== 'NoSuchUpload' && err.name !== 'NoSuchBucket') {
                            process.stdout.write(`Error in afterEach: ${err}\n`);
                            throw err;
                        }
                    }),
            );

            it('should copy a part from a source bucket to a different ' + 'destination bucket', () => {
                process.stdout.write('Entered first mpu test\n');
                return s3
                    .send(
                        new UploadPartCopyCommand({
                            Bucket: destBucketName,
                            Key: destObjName,
                            CopySource: `${sourceBucketName}/${sourceMpuKey}`,
                            PartNumber: 1,
                            UploadId: uploadId,
                        }),
                    )
                    .then(res => {
                        assert.strictEqual(res.CopyPartResult.ETag, totalMpuObjectHash);
                        assert(res.CopyPartResult.LastModified);
                    });
            });

            it(
                'should copy two parts from a source bucket to a different ' +
                    'destination bucket and complete the MPU',
                () => {
                    process.stdout.write('Putting first part in MPU test\n');
                    return s3
                        .send(
                            new UploadPartCopyCommand({
                                Bucket: destBucketName,
                                Key: destObjName,
                                CopySource: `${sourceBucketName}/${sourceMpuKey}`,
                                PartNumber: 1,
                                UploadId: uploadId,
                            }),
                        )
                        .then(res => {
                            assert.strictEqual(res.CopyPartResult.ETag, totalMpuObjectHash);
                            assert(res.CopyPartResult.LastModified);
                        })
                        .then(() => {
                            process.stdout.write('Putting second part in MPU test\n');
                            return s3
                                .send(
                                    new UploadPartCopyCommand({
                                        Bucket: destBucketName,
                                        Key: destObjName,
                                        CopySource: `${sourceBucketName}/${sourceMpuKey}`,
                                        PartNumber: 2,
                                        UploadId: uploadId,
                                    }),
                                )
                                .then(res => {
                                    assert.strictEqual(res.CopyPartResult.ETag, totalMpuObjectHash);
                                    assert(res.CopyPartResult.LastModified);
                                })
                                .then(() => {
                                    process.stdout.write('Completing MPU\n');
                                    return s3
                                        .send(
                                            new CompleteMultipartUploadCommand({
                                                Bucket: destBucketName,
                                                Key: destObjName,
                                                UploadId: uploadId,
                                                MultipartUpload: {
                                                    Parts: [
                                                        { ETag: totalMpuObjectHash, PartNumber: 1 },
                                                        { ETag: totalMpuObjectHash, PartNumber: 2 },
                                                    ],
                                                },
                                            }),
                                        )
                                        .then(res => {
                                            assert.strictEqual(res.Bucket, destBucketName);
                                            assert.strictEqual(res.Key, destObjName);
                                            // combined ETag returned by AWS (combination of part ETags
                                            // with number of parts at the end)
                                            assert.strictEqual(res.ETag, '"5bba96810ff449d94aa8f5c5a859b0cb-2"');
                                        })
                                        .catch(err => {
                                            checkNoError(err);
                                        });
                                });
                        });
                },
            );

            it(
                'should copy two parts with range headers from a source ' +
                    'bucket to a different destination bucket and ' +
                    'complete the MPU',
                () => {
                    process.stdout.write('Putting first part in MPU range test\n');
                    const part1ETag = '"b1e0d096c8f0670c5367d131e392b84a"';
                    const part2ETag = '"a2468d5c0ec2d4d5fc13b73beb63080a"';
                    // combined ETag returned by AWS (combination of part ETags
                    // with number of parts at the end)
                    const finalCombinedETag = '"e08ede4e8b942e18537cb2289f613ae3-2"';
                    return s3
                        .send(
                            new UploadPartCopyCommand({
                                Bucket: destBucketName,
                                Key: destObjName,
                                CopySource: `${sourceBucketName}/${sourceMpuKey}`,
                                PartNumber: 1,
                                UploadId: uploadId,
                                CopySourceRange: 'bytes=5242890-15242880',
                            }),
                        )
                        .then(res => {
                            assert.strictEqual(res.CopyPartResult.ETag, part1ETag);
                            assert(res.CopyPartResult.LastModified);
                        })
                        .then(() => {
                            process.stdout.write('Putting second part in MPU test\n');
                            return s3
                                .send(
                                    new UploadPartCopyCommand({
                                        Bucket: destBucketName,
                                        Key: destObjName,
                                        CopySource: `${sourceBucketName}/${sourceMpuKey}`,
                                        PartNumber: 2,
                                        UploadId: uploadId,
                                        CopySourceRange: 'bytes=15242891-30242991',
                                    }),
                                )
                                .then(res => {
                                    assert.strictEqual(res.CopyPartResult.ETag, part2ETag);
                                    assert(res.CopyPartResult.LastModified);
                                })
                                .then(() => {
                                    process.stdout.write('Completing MPU\n');
                                    return s3
                                        .send(
                                            new CompleteMultipartUploadCommand({
                                                Bucket: destBucketName,
                                                Key: destObjName,
                                                UploadId: uploadId,
                                                MultipartUpload: {
                                                    Parts: [
                                                        { ETag: part1ETag, PartNumber: 1 },
                                                        { ETag: part2ETag, PartNumber: 2 },
                                                    ],
                                                },
                                            }),
                                        )
                                        .then(res => {
                                            assert.strictEqual(res.Bucket, destBucketName);
                                            assert.strictEqual(res.Key, destObjName);
                                            assert.strictEqual(res.ETag, finalCombinedETag);
                                        })
                                        .then(() => {
                                            process.stdout.write('Getting new object\n');
                                            return s3
                                                .send(
                                                    new GetObjectCommand({
                                                        Bucket: destBucketName,
                                                        Key: destObjName,
                                                    }),
                                                )
                                                .then(res => {
                                                    assert.strictEqual(res.ContentLength, 25000092);
                                                    assert.strictEqual(res.ETag, finalCombinedETag);
                                                })
                                                .catch(err => {
                                                    checkNoError(err);
                                                });
                                        });
                                });
                        });
                },
            );

            it('should overwrite an existing part by copying a part', () => {
                // AWS response etag for this completed MPU
                const finalObjETag = '"db77ebbae9e9f5a244a26b86193ad818-1"';
                process.stdout.write('Putting first part in MPU test\n');
                return s3
                    .send(
                        new UploadPartCopyCommand({
                            Bucket: destBucketName,
                            Key: destObjName,
                            CopySource: `${sourceBucketName}/${sourceMpuKey}`,
                            PartNumber: 1,
                            UploadId: uploadId,
                        }),
                    )
                    .then(res => {
                        assert.strictEqual(res.CopyPartResult.ETag, totalMpuObjectHash);
                        assert(res.CopyPartResult.LastModified);
                    })
                    .then(() => {
                        process.stdout.write('Overwriting first part in MPU test\n');
                        return s3
                            .send(
                                new UploadPartCopyCommand({
                                    Bucket: destBucketName,
                                    Key: destObjName,
                                    CopySource: `${sourceBucketName}/${sourceObjName}`,
                                    PartNumber: 1,
                                    UploadId: uploadId,
                                }),
                            )
                            .then(res => {
                                assert.strictEqual(res.CopyPartResult.ETag, etag);
                                assert(res.CopyPartResult.LastModified);
                                process.stdout.write('Completing MPU\n');
                                return s3
                                    .send(
                                        new CompleteMultipartUploadCommand({
                                            Bucket: destBucketName,
                                            Key: destObjName,
                                            UploadId: uploadId,
                                            MultipartUpload: {
                                                Parts: [{ ETag: etag, PartNumber: 1 }],
                                            },
                                        }),
                                    )
                                    .then(res => {
                                        assert.strictEqual(res.Bucket, destBucketName);
                                        assert.strictEqual(res.Key, destObjName);
                                        assert.strictEqual(res.ETag, finalObjETag);
                                    })
                                    .then(() => {
                                        process.stdout.write('Getting object put by MPU with ' + 'overwrite part\n');
                                        return s3
                                            .send(
                                                new GetObjectCommand({
                                                    Bucket: destBucketName,
                                                    Key: destObjName,
                                                }),
                                            )
                                            .then(res => {
                                                assert.strictEqual(res.ETag, finalObjETag);
                                            })
                                            .catch(err => {
                                                checkNoError(err);
                                            });
                                    });
                            });
                    });
            });

            it(
                'should not corrupt object if overwriting an existing part by copying a part ' +
                    'while the MPU is being completed',
                async () => {
                    const finalObjETag = '"db77ebbae9e9f5a244a26b86193ad818-1"';
                    process.stdout.write('Putting first part in MPU test\n');
                    const randomDestObjName = `copycatobject${Math.floor(Math.random() * 100000)}`;

                    const initiateRes = await s3.send(
                        new CreateMultipartUploadCommand({
                            Bucket: destBucketName,
                            Key: randomDestObjName,
                        }),
                    );
                    const uploadId = initiateRes.UploadId;

                    const res = await s3.send(
                        new UploadPartCopyCommand({
                            Bucket: destBucketName,
                            Key: randomDestObjName,
                            CopySource: `${sourceBucketName}/${sourceObjName}`,
                            PartNumber: 1,
                            UploadId: uploadId,
                        }),
                    );
                    assert.strictEqual(res.CopyPartResult.ETag, etag);
                    assert(res.CopyPartResult.LastModified);

                    process.stdout.write('Overwriting first part in MPU test and completing MPU at the same time\n');
                    const [completeRes, uploadRes] = await Promise.all([
                        s3
                            .send(
                                new CompleteMultipartUploadCommand({
                                    Bucket: destBucketName,
                                    Key: randomDestObjName,
                                    UploadId: uploadId,
                                    MultipartUpload: {
                                        Parts: [{ ETag: etag, PartNumber: 1 }],
                                    },
                                }),
                            )
                            .catch(err => {
                                throw err;
                            }),
                        s3
                            .send(
                                new UploadPartCopyCommand({
                                    Bucket: destBucketName,
                                    Key: randomDestObjName,
                                    CopySource: `${sourceBucketName}/${sourceObjName}`,
                                    PartNumber: 1,
                                    UploadId: uploadId,
                                }),
                            )
                            .catch(err => {
                                const completeMPUFinishedEarlier =
                                    err.name === 'NoSuchKey' || err.name === 'NoSuchUpload';
                                if (completeMPUFinishedEarlier) {
                                    return Promise.resolve(null);
                                }
                                throw err;
                            }),
                    ]);

                    if (uploadRes !== null) {
                        assert.strictEqual(uploadRes.CopyPartResult.ETag, etag);
                        assert(uploadRes.CopyPartResult.LastModified);
                    }
                    assert.strictEqual(completeRes.Bucket, destBucketName);
                    assert.strictEqual(completeRes.Key, randomDestObjName);
                    assert.strictEqual(completeRes.ETag, finalObjETag);
                    process.stdout.write('Getting object put by MPU with overwrite part\n');
                    const resGet = await s3.send(
                        new GetObjectCommand({
                            Bucket: destBucketName,
                            Key: randomDestObjName,
                        }),
                    );
                    assert.strictEqual(resGet.ETag, finalObjETag);
                },
            );
        });

        it('should return an error if no such upload initiated', () =>
            s3
                .send(
                    new UploadPartCopyCommand({
                        Bucket: destBucketName,
                        Key: destObjName,
                        CopySource: `${sourceBucketName}/${sourceObjName}`,
                        PartNumber: 1,
                        UploadId: 'madeupuploadid444233232',
                    }),
                )
                .catch(err => {
                    checkError(err, 'NoSuchUpload');
                }));

        it('should return an error if attempt to copy from nonexistent bucket', () =>
            s3
                .send(
                    new UploadPartCopyCommand({
                        Bucket: destBucketName,
                        Key: destObjName,
                        CopySource: `nobucket453234/${sourceObjName}`,
                        PartNumber: 1,
                        UploadId: uploadId,
                    }),
                )
                .catch(err => {
                    checkError(err, 'NoSuchBucket');
                }));

        it('should return an error if attempt to copy to nonexistent bucket', () =>
            s3
                .send(
                    new UploadPartCopyCommand({
                        Bucket: 'nobucket453234',
                        Key: destObjName,
                        CopySource: `${sourceBucketName}/${sourceObjName}`,
                        PartNumber: 1,
                        UploadId: uploadId,
                    }),
                )
                .catch(err => {
                    checkError(err, 'NoSuchBucket');
                }));

        it('should return an error if attempt to copy nonexistent object', () =>
            s3
                .send(
                    new UploadPartCopyCommand({
                        Bucket: destBucketName,
                        Key: destObjName,
                        CopySource: `${sourceBucketName}/nokey`,
                        PartNumber: 1,
                        UploadId: uploadId,
                    }),
                )
                .catch(err => {
                    checkError(err, 'NoSuchKey');
                }));

        it('should return an error if use invalid part number', () =>
            s3
                .send(
                    new UploadPartCopyCommand({
                        Bucket: destBucketName,
                        Key: destObjName,
                        CopySource: `${sourceBucketName}/nokey`,
                        PartNumber: 10001,
                        UploadId: uploadId,
                    }),
                )
                .catch(err => {
                    checkError(err, 'InvalidArgument');
                }));

        const describeColdStorage = hasColdStorage ? describe : describe.skip;
        describeColdStorage('with cold storage', () => {
            it('should not copy a part of a cold object', done => {
                const archive = {
                    archiveInfo: {
                        archiveId: '97a71dfe-49c1-4cca-840a-69199e0b0322',
                        archiveVersion: 5577006791947779,
                    },
                };
                fakeMetadataArchive(sourceBucketName, sourceObjName, undefined, archive, err => {
                    assert.ifError(err);
                    s3.send(
                        new UploadPartCopyCommand({
                            Bucket: destBucketName,
                            Key: destObjName,
                            CopySource: `${sourceBucketName}/${sourceObjName}`,
                            PartNumber: 1,
                            UploadId: uploadId,
                        }),
                    )
                        .then(() => {
                            done(new Error('Expected failure but got success'));
                        })
                        .catch(err => {
                            assert.strictEqual(err.$metadata.httpStatusCode, 403);
                            done();
                        });
                });
            });

            it("should copy a part of an object when it's transitioning to cold", done => {
                fakeMetadataTransition(sourceBucketName, sourceObjName, undefined, err => {
                    assert.ifError(err);
                    s3.send(
                        new UploadPartCopyCommand({
                            Bucket: destBucketName,
                            Key: destObjName,
                            CopySource: `${sourceBucketName}/${sourceObjName}`,
                            PartNumber: 1,
                            UploadId: uploadId,
                        }),
                    )
                        .then(res => {
                            assert.strictEqual(res.CopyPartResult.ETag, etag);
                            assert(res.CopyPartResult.LastModified);
                            done();
                        })
                        .catch(err => {
                            checkNoError(err);
                            done(err);
                        });
                });
            });

            it('should copy a part of a restored object', done => {
                const archiveCompleted = {
                    archiveInfo: {},
                    restoreRequestedAt: new Date(0),
                    restoreRequestedDays: 5,
                    restoreCompletedAt: new Date(10),
                    restoreWillExpireAt: new Date(10 + 5 * 24 * 60 * 60 * 1000),
                };
                fakeMetadataArchive(sourceBucketName, sourceObjName, undefined, archiveCompleted, err => {
                    assert.ifError(err);
                    s3.send(
                        new UploadPartCopyCommand({
                            Bucket: destBucketName,
                            Key: destObjName,
                            CopySource: `${sourceBucketName}/${sourceObjName}`,
                            PartNumber: 1,
                            UploadId: uploadId,
                        }),
                    )
                        .then(res => {
                            assert.strictEqual(res.CopyPartResult.ETag, etag);
                            assert(res.CopyPartResult.LastModified);
                            done();
                        })
                        .catch(err => {
                            checkNoError(err);
                            done(err);
                        });
                });
            });
        });

        describe('copying parts by another account', () => {
            const otherAccountBucket = 'otheraccountbucket42342342342';
            const otherAccountKey = 'key';
            let otherAccountUploadId;

            beforeEach(() => {
                process.stdout.write('In other account before each\n');
                return otherAccountS3
                    .send(new CreateBucketCommand({ Bucket: otherAccountBucket }))
                    .catch(err => {
                        process.stdout.write('Error creating other account ' + `bucket: ${err}\n`);
                        throw err;
                    })
                    .then(() => {
                        process.stdout.write('Initiating other account MPU\n');
                        return otherAccountS3.send(
                            new CreateMultipartUploadCommand({
                                Bucket: otherAccountBucket,
                                Key: otherAccountKey,
                            }),
                        );
                    })
                    .then(initiateRes => {
                        otherAccountUploadId = initiateRes.UploadId;
                    })
                    .catch(err => {
                        process.stdout.write('Error in other account ' + `beforeEach: ${err}\n`);
                        throw err;
                    });
            });

            afterEach(() =>
                otherAccountBucketUtility
                    .empty(otherAccountBucket)
                    .then(() =>
                        otherAccountS3.send(
                            new AbortMultipartUploadCommand({
                                Bucket: otherAccountBucket,
                                Key: otherAccountKey,
                                UploadId: otherAccountUploadId,
                            }),
                        ),
                    )
                    .catch(err => {
                        if (err.name !== 'NoSuchUpload') {
                            process.stdout.write('Error in other account ' + `afterEach: ${err}\n`);
                            throw err;
                        }
                    })
                    .then(() => {
                        otherAccountBucketUtility.deleteOne(otherAccountBucket);
                    }),
            );

            it(
                'should not allow an account without read persmission on the ' + 'source object to copy the object',
                () =>
                    otherAccountS3
                        .send(
                            new UploadPartCopyCommand({
                                Bucket: otherAccountBucket,
                                Key: otherAccountKey,
                                CopySource: `${sourceBucketName}/${sourceObjName}`,
                                PartNumber: 1,
                                UploadId: otherAccountUploadId,
                            }),
                        )
                        .catch(err => {
                            checkError(err, 'AccessDenied');
                        }),
            );

            it(
                'should not allow an account without write persmission on the ' +
                    'destination bucket to upload part copy the object',
                () => {
                    otherAccountS3
                        .send(new PutObjectCommand({ Bucket: otherAccountBucket, Key: otherAccountKey, Body: '' }))
                        .then(() =>
                            otherAccountS3
                                .send(
                                    new UploadPartCopyCommand({
                                        Bucket: destBucketName,
                                        Key: destObjName,
                                        CopySource: `${otherAccountBucket}/${otherAccountKey}`,
                                        PartNumber: 1,
                                        UploadId: uploadId,
                                    }),
                                )
                                .catch(err => checkError(err, 'AccessDenied')),
                        );
                },
            );

            it(
                'should allow an account with read permission on the ' +
                    'source object and write permission on the destination ' +
                    'bucket to upload part copy the object',
                () =>
                    s3
                        .send(
                            new PutObjectAclCommand({
                                Bucket: sourceBucketName,
                                Key: sourceObjName,
                                ACL: 'public-read',
                            }),
                        )
                        .then(() =>
                            otherAccountS3
                                .send(
                                    new UploadPartCopyCommand({
                                        Bucket: otherAccountBucket,
                                        Key: otherAccountKey,
                                        CopySource: `${sourceBucketName}/${sourceObjName}`,
                                        PartNumber: 1,
                                        UploadId: otherAccountUploadId,
                                    }),
                                )
                                .catch(err => {
                                    checkNoError(err);
                                }),
                        ),
            );
        });
    });
});
