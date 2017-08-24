const assert = require('assert');
const crypto = require('crypto');

const Promise = require('bluebird');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const { createEncryptedBucketPromise } =
    require('../../lib/utility/createEncryptedBucket');

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
    assert.equal(err, null,
        `Expected success, got error ${JSON.stringify(err)}`);
}

function checkError(err, code) {
    assert.notEqual(err, null, 'Expected failure but got success');
    assert.strictEqual(err.code, code);
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
            if (process.env.ENABLE_KMS_ENCRYPTION === 'true') {
                s3.createBucketAsync = createEncryptedBucketPromise;
            }
            return s3.createBucketAsync({ Bucket: sourceBucketName })
            .catch(err => {
                process.stdout.write(`Error creating source bucket: ${err}\n`);
                throw err;
            }).then(() =>
                s3.createBucketAsync({ Bucket: destBucketName })
            ).catch(err => {
                process.stdout.write(`Error creating dest bucket: ${err}\n`);
                throw err;
            })
            .then(() =>
                s3.putObjectAsync({
                    Bucket: sourceBucketName,
                    Key: sourceObjName,
                    Body: content,
                }))
            .then(res => {
                etag = res.ETag;
                return s3.headObjectAsync({
                    Bucket: sourceBucketName,
                    Key: sourceObjName,
                });
            }).then(() =>
            s3.createMultipartUploadAsync({
                Bucket: destBucketName,
                Key: destObjName,
            })).then(iniateRes => {
                uploadId = iniateRes.UploadId;
            }).catch(err => {
                process.stdout.write(`Error in outer beforeEach: ${err}\n`);
                throw err;
            });
        });

        afterEach(() => bucketUtil.empty(sourceBucketName)
            .then(() => bucketUtil.empty(destBucketName))
            .then(() => s3.abortMultipartUploadAsync({
                Bucket: destBucketName,
                Key: destObjName,
                UploadId: uploadId,
            }))
            .catch(err => {
                if (err.code !== 'NoSuchUpload') {
                    process.stdout.write(`Error in afterEach: ${err}\n`);
                    throw err;
                }
            })
            .then(() => bucketUtil.deleteMany([sourceBucketName,
                destBucketName]))
            );


        it('should copy a part from a source bucket to a different ' +
            'destination bucket', done => {
            s3.uploadPartCopy({ Bucket: destBucketName,
                Key: destObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}`,
                PartNumber: 1,
                UploadId: uploadId,
            },
                (err, res) => {
                    checkNoError(err);
                    assert.strictEqual(res.ETag, etag);
                    assert(res.LastModified);
                    done();
                });
        });

        it('should copy a part from a source bucket to a different ' +
            'destination bucket and complete the MPU', done => {
            s3.uploadPartCopy({ Bucket: destBucketName,
                Key: destObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}`,
                PartNumber: 1,
                UploadId: uploadId,
            },
                (err, res) => {
                    checkNoError(err);
                    assert.strictEqual(res.ETag, etag);
                    assert(res.LastModified);
                    s3.completeMultipartUpload({
                        Bucket: destBucketName,
                        Key: destObjName,
                        UploadId: uploadId,
                        MultipartUpload: {
                            Parts: [
                                { ETag: etag, PartNumber: 1 },
                            ],
                        },
                    }, (err, res) => {
                        checkNoError(err);
                        assert.strictEqual(res.Bucket, destBucketName);
                        assert.strictEqual(res.Key, destObjName);
                        // AWS confirmed final ETag for MPU
                        assert.strictEqual(res.ETag,
                            '"db77ebbae9e9f5a244a26b86193ad818-1"');
                        done();
                    });
                });
        });

        it('should return EntityTooLarge error if attempt to copy ' +
            'object larger than max and do not specify smaller ' +
            'range in request', done => {
            s3.putObject({
                Bucket: sourceBucketName,
                Key: sourceObjName,
                Body: Buffer.alloc(oneHundredMBPlus11, 'packing'),
            }, err => {
                checkNoError(err);
                s3.uploadPartCopy({ Bucket: destBucketName,
                    Key: destObjName,
                    CopySource: `${sourceBucketName}/${sourceObjName}`,
                    PartNumber: 1,
                    UploadId: uploadId,
                },
                    err => {
                        checkError(err, 'EntityTooLarge');
                        done();
                    });
            });
        });

        it('should return EntityTooLarge error if attempt to copy ' +
            'object larger than max and specify too large ' +
            'range in request', done => {
            s3.putObject({
                Bucket: sourceBucketName,
                Key: sourceObjName,
                Body: Buffer.alloc(oneHundredMBPlus11, 'packing'),
            }, err => {
                checkNoError(err);
                s3.uploadPartCopy({ Bucket: destBucketName,
                    Key: destObjName,
                    CopySource: `${sourceBucketName}/${sourceObjName}`,
                    PartNumber: 1,
                    UploadId: uploadId,
                    CopySourceRange: `bytes=0-${oneHundredMBPlus11}`,
                },
                    err => {
                        checkError(err, 'EntityTooLarge');
                        done();
                    });
            });
        });

        it('should succeed if attempt to copy ' +
            'object larger than max but specify acceptable ' +
            'range in request', done => {
            s3.putObject({
                Bucket: sourceBucketName,
                Key: sourceObjName,
                Body: Buffer.alloc(oneHundredMBPlus11, 'packing'),
            }, err => {
                checkNoError(err);
                s3.uploadPartCopy({ Bucket: destBucketName,
                    Key: destObjName,
                    CopySource: `${sourceBucketName}/${sourceObjName}`,
                    PartNumber: 1,
                    UploadId: uploadId,
                    CopySourceRange: 'bytes=0-100',
                },
                    err => {
                        checkNoError(err);
                        done();
                    });
            });
        });

        it('should copy a 0 byte object part from a source bucket to a ' +
            'different destination bucket and complete the MPU', done => {
            const emptyFileETag = '"d41d8cd98f00b204e9800998ecf8427e"';
            s3.putObject({
                Bucket: sourceBucketName,
                Key: sourceObjName,
                Body: '',
            }, () => {
                s3.uploadPartCopy({ Bucket: destBucketName,
                    Key: destObjName,
                    CopySource: `${sourceBucketName}/${sourceObjName}`,
                    PartNumber: 1,
                    UploadId: uploadId,
                },
                    (err, res) => {
                        checkNoError(err);
                        assert.strictEqual(res.ETag, emptyFileETag);
                        assert(res.LastModified);
                        s3.completeMultipartUpload({
                            Bucket: destBucketName,
                            Key: destObjName,
                            UploadId: uploadId,
                            MultipartUpload: {
                                Parts: [
                                    { ETag: emptyFileETag, PartNumber: 1 },
                                ],
                            },
                        }, (err, res) => {
                            checkNoError(err);
                            assert.strictEqual(res.Bucket, destBucketName);
                            assert.strictEqual(res.Key, destObjName);
                            // AWS confirmed final ETag for MPU
                            assert.strictEqual(res.ETag,
                                '"59adb24ef3cdbe0297f05b395827453f-1"');
                            done();
                        });
                    });
            });
        });

        it('should copy a part using a range header from a source bucket ' +
            'to a different destination bucket and complete the MPU', done => {
            const rangeETag = '"ac1be00f1f162e20d58099eec2ea1c70"';
            // AWS confirmed final ETag for MPU
            const finalMpuETag = '"bff2a6af3adfd8e107a06de01d487176-1"';
            s3.uploadPartCopy({ Bucket: destBucketName,
                Key: destObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}`,
                PartNumber: 1,
                CopySourceRange: 'bytes=0-3',
                UploadId: uploadId,
            },
                (err, res) => {
                    checkNoError(err);
                    assert.strictEqual(res.ETag, rangeETag);
                    assert(res.LastModified);
                    s3.completeMultipartUpload({
                        Bucket: destBucketName,
                        Key: destObjName,
                        UploadId: uploadId,
                        MultipartUpload: {
                            Parts: [
                                { ETag: rangeETag, PartNumber: 1 },
                            ],
                        },
                    }, (err, res) => {
                        checkNoError(err);
                        assert.strictEqual(res.Bucket, destBucketName);
                        assert.strictEqual(res.Key, destObjName);
                        assert.strictEqual(res.ETag, finalMpuETag);
                        s3.getObject({
                            Bucket: destBucketName,
                            Key: destObjName,
                        }, (err, res) => {
                            checkNoError(err);
                            assert.strictEqual(res.ETag, finalMpuETag);
                            assert.strictEqual(res.ContentLength, '4');
                            assert.strictEqual(res.Body.toString(), 'I am');
                            done();
                        });
                    });
                });
        });

        describe('When copy source was put by MPU', () => {
            let sourceMpuId;
            const sourceMpuKey = 'sourceMpuKey';
            // total hash for sourceMpuKey when MPU completed
            // (confirmed with AWS)
            const totalMpuObjectHash =
                '"9b0de95bd76728c778b9e25fd7ce2ef7"';

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
                return s3.createMultipartUploadAsync({
                    Bucket: sourceBucketName,
                    Key: sourceMpuKey,
                }).then(iniateRes => {
                    sourceMpuId = iniateRes.UploadId;
                }).catch(err => {
                    process.stdout.write(`Error initiating MPU ' +
                    'in MPU beforeEach: ${err}\n`);
                    throw err;
                }).then(() => {
                    const partUploads = [];
                    for (let i = 1; i < 10; i++) {
                        const partBuffHere = i % 2 ? partBuff : otherPartBuff;
                        const partHashHere = i % 2 ? partHash : otherPartHash;
                        partUploads.push(s3.uploadPartAsync({
                            Bucket: sourceBucketName,
                            Key: sourceMpuKey,
                            PartNumber: i,
                            UploadId: sourceMpuId,
                            Body: partBuffHere,
                        }));
                        parts.push({
                            ETag: partHashHere,
                            PartNumber: i,
                        });
                    }
                    process.stdout.write('about to put parts');
                    return Promise.all(partUploads);
                }).catch(err => {
                    process.stdout.write(`Error putting parts in ' +
                    'MPU beforeEach: ${err}\n`);
                    throw err;
                }).then(() => {
                    process.stdout.write('completing mpu');
                    return s3.completeMultipartUploadAsync({
                        Bucket: sourceBucketName,
                        Key: sourceMpuKey,
                        UploadId: sourceMpuId,
                        MultipartUpload: {
                            Parts: parts,
                        },
                    });
                }).then(() => {
                    process.stdout.write('finished completing mpu');
                }).catch(err => {
                    process.stdout.write(`Error in MPU beforeEach: ${err}\n`);
                    throw err;
                });
            });

            afterEach(() => s3.abortMultipartUploadAsync({
                Bucket: sourceBucketName,
                Key: sourceMpuKey,
                UploadId: sourceMpuId,
            }).catch(err => {
                if (err.code !== 'NoSuchUpload'
                && err.code !== 'NoSuchBucket') {
                    process.stdout.write(`Error in afterEach: ${err}\n`);
                    throw err;
                }
            }));

            it('should copy a part from a source bucket to a different ' +
                'destination bucket', done => {
                process.stdout.write('Entered first mpu test');
                return s3.uploadPartCopy({ Bucket: destBucketName,
                    Key: destObjName,
                    CopySource: `${sourceBucketName}/${sourceMpuKey}`,
                    PartNumber: 1,
                    UploadId: uploadId,
                },
                    (err, res) => {
                        checkNoError(err);
                        assert.strictEqual(res.ETag,
                            totalMpuObjectHash);
                        assert(res.LastModified);
                        done();
                    });
            });

            it('should copy two parts from a source bucket to a different ' +
                'destination bucket and complete the MPU', () => {
                process.stdout.write('Putting first part in MPU test');
                return s3.uploadPartCopyAsync({ Bucket: destBucketName,
                    Key: destObjName,
                    CopySource: `${sourceBucketName}/${sourceMpuKey}`,
                    PartNumber: 1,
                    UploadId: uploadId,
                }).then(res => {
                    assert.strictEqual(res.ETag, totalMpuObjectHash);
                    assert(res.LastModified);
                }).then(() => {
                    process.stdout.write('Putting second part in MPU test');
                    return s3.uploadPartCopyAsync({ Bucket: destBucketName,
                        Key: destObjName,
                        CopySource: `${sourceBucketName}/${sourceMpuKey}`,
                        PartNumber: 2,
                        UploadId: uploadId,
                }).then(res => {
                    assert.strictEqual(res.ETag, totalMpuObjectHash);
                    assert(res.LastModified);
                }).then(() => {
                    process.stdout.write('Completing MPU');
                    return s3.completeMultipartUploadAsync({
                        Bucket: destBucketName,
                        Key: destObjName,
                        UploadId: uploadId,
                        MultipartUpload: {
                            Parts: [
                                { ETag: totalMpuObjectHash, PartNumber: 1 },
                                { ETag: totalMpuObjectHash, PartNumber: 2 },
                            ],
                        },
                    });
                }).then(res => {
                    assert.strictEqual(res.Bucket, destBucketName);
                    assert.strictEqual(res.Key, destObjName);
                    // combined ETag returned by AWS (combination of part ETags
                    // with number of parts at the end)
                    assert.strictEqual(res.ETag,
                        '"5bba96810ff449d94aa8f5c5a859b0cb-2"');
                }).catch(err => {
                    checkNoError(err);
                });
                });
            });

            it('should copy two parts with range headers from a source ' +
                'bucket to a different destination bucket and ' +
                'complete the MPU', () => {
                process.stdout.write('Putting first part in MPU range test');
                const part1ETag = '"b1e0d096c8f0670c5367d131e392b84a"';
                const part2ETag = '"a2468d5c0ec2d4d5fc13b73beb63080a"';
                // combined ETag returned by AWS (combination of part ETags
                // with number of parts at the end)
                const finalCombinedETag =
                    '"e08ede4e8b942e18537cb2289f613ae3-2"';
                return s3.uploadPartCopyAsync({ Bucket: destBucketName,
                    Key: destObjName,
                    CopySource: `${sourceBucketName}/${sourceMpuKey}`,
                    PartNumber: 1,
                    UploadId: uploadId,
                    CopySourceRange: 'bytes=5242890-15242880',
                }).then(res => {
                    assert.strictEqual(res.ETag, part1ETag);
                    assert(res.LastModified);
                }).then(() => {
                    process.stdout.write('Putting second part in MPU test');
                    return s3.uploadPartCopyAsync({ Bucket: destBucketName,
                        Key: destObjName,
                        CopySource: `${sourceBucketName}/${sourceMpuKey}`,
                        PartNumber: 2,
                        UploadId: uploadId,
                        CopySourceRange: 'bytes=15242891-30242991',
                }).then(res => {
                    assert.strictEqual(res.ETag, part2ETag);
                    assert(res.LastModified);
                }).then(() => {
                    process.stdout.write('Completing MPU');
                    return s3.completeMultipartUploadAsync({
                        Bucket: destBucketName,
                        Key: destObjName,
                        UploadId: uploadId,
                        MultipartUpload: {
                            Parts: [
                                { ETag: part1ETag, PartNumber: 1 },
                                { ETag: part2ETag, PartNumber: 2 },
                            ],
                        },
                    });
                }).then(res => {
                    assert.strictEqual(res.Bucket, destBucketName);
                    assert.strictEqual(res.Key, destObjName);
                    assert.strictEqual(res.ETag, finalCombinedETag);
                }).then(() => {
                    process.stdout.write('Getting new object');
                    return s3.getObjectAsync({
                        Bucket: destBucketName,
                        Key: destObjName,
                    });
                }).then(res => {
                    assert.strictEqual(res.ContentLength, '25000092');
                    assert.strictEqual(res.ETag, finalCombinedETag);
                })
                .catch(err => {
                    checkNoError(err);
                });
                });
            });

            it('should overwrite an existing part by copying a part', () => {
                // AWS response etag for this completed MPU
                const finalObjETag = '"db77ebbae9e9f5a244a26b86193ad818-1"';
                process.stdout.write('Putting first part in MPU test');
                return s3.uploadPartCopyAsync({ Bucket: destBucketName,
                    Key: destObjName,
                    CopySource: `${sourceBucketName}/${sourceMpuKey}`,
                    PartNumber: 1,
                    UploadId: uploadId,
                }).then(res => {
                    assert.strictEqual(res.ETag, totalMpuObjectHash);
                    assert(res.LastModified);
                }).then(() => {
                    process.stdout.write('Overwriting first part in MPU test');
                    return s3.uploadPartCopyAsync({ Bucket: destBucketName,
                        Key: destObjName,
                        CopySource: `${sourceBucketName}/${sourceObjName}`,
                        PartNumber: 1,
                        UploadId: uploadId });
                }).then(res => {
                    assert.strictEqual(res.ETag, etag);
                    assert(res.LastModified);
                }).then(() => {
                    process.stdout.write('Completing MPU');
                    return s3.completeMultipartUploadAsync({
                        Bucket: destBucketName,
                        Key: destObjName,
                        UploadId: uploadId,
                        MultipartUpload: {
                            Parts: [
                                { ETag: etag, PartNumber: 1 },
                            ],
                        },
                    });
                }).then(res => {
                    assert.strictEqual(res.Bucket, destBucketName);
                    assert.strictEqual(res.Key, destObjName);
                    assert.strictEqual(res.ETag, finalObjETag);
                }).then(() => {
                    process.stdout.write('Getting object put by MPU with ' +
                    'overwrite part');
                    return s3.getObjectAsync({
                        Bucket: destBucketName,
                        Key: destObjName,
                    });
                }).then(res => {
                    assert.strictEqual(res.ETag, finalObjETag);
                }).catch(err => {
                    checkNoError(err);
                });
            });
        });

        it('should return an error if no such upload initiated',
            done => {
                s3.uploadPartCopy({ Bucket: destBucketName, Key: destObjName,
                    CopySource: `${sourceBucketName}/${sourceObjName}`,
                    PartNumber: 1,
                    UploadId: 'madeupuploadid444233232',
            },
                err => {
                    checkError(err, 'NoSuchUpload');
                    done();
                });
            });

        it('should return an error if attempt to copy from nonexistent bucket',
            done => {
                s3.uploadPartCopy({ Bucket: destBucketName, Key: destObjName,
                    CopySource: `nobucket453234/${sourceObjName}`,
                    PartNumber: 1,
                    UploadId: uploadId,
            },
                err => {
                    checkError(err, 'NoSuchBucket');
                    done();
                });
            });

        it('should return an error if attempt to copy to nonexistent bucket',
            done => {
                s3.uploadPartCopy({ Bucket: 'nobucket453234', Key: destObjName,
                    CopySource: `${sourceBucketName}/${sourceObjName}`,
                    PartNumber: 1,
                    UploadId: uploadId,
            },
                err => {
                    checkError(err, 'NoSuchBucket');
                    done();
                });
            });

        it('should return an error if attempt to copy nonexistent object',
            done => {
                s3.uploadPartCopy({ Bucket: destBucketName, Key: destObjName,
                    CopySource: `${sourceBucketName}/nokey`,
                    PartNumber: 1,
                    UploadId: uploadId,
            },
                err => {
                    checkError(err, 'NoSuchKey');
                    done();
                });
            });

        it('should return an error if use invalid part number',
            done => {
                s3.uploadPartCopy({ Bucket: destBucketName, Key: destObjName,
                    CopySource: `${sourceBucketName}/nokey`,
                    PartNumber: 10001,
                    UploadId: uploadId,
            },
                err => {
                    checkError(err, 'InvalidArgument');
                    done();
                });
            });

        describe('copying parts by another account', () => {
            const otherAccountBucket = 'otheraccountbucket42342342342';
            const otherAccountKey = 'key';
            let otherAccountUploadId;

            beforeEach(() => {
                process.stdout.write('In other account before each');
                return otherAccountS3.createBucketAsync({ Bucket:
                otherAccountBucket })
                .catch(err => {
                    process.stdout.write('Error creating other account ' +
                    `bucket: ${err}\n`);
                    throw err;
                }).then(() => {
                    process.stdout.write('Initiating other account MPU');
                    return otherAccountS3.createMultipartUploadAsync({
                        Bucket: otherAccountBucket,
                        Key: otherAccountKey,
                    });
                }).then(iniateRes => {
                    otherAccountUploadId = iniateRes.UploadId;
                }).catch(err => {
                    process.stdout.write('Error in other account ' +
                    `beforeEach: ${err}\n`);
                    throw err;
                });
            });

            afterEach(() => otherAccountBucketUtility.empty(otherAccountBucket)
                .then(() => otherAccountS3.abortMultipartUploadAsync({
                    Bucket: otherAccountBucket,
                    Key: otherAccountKey,
                    UploadId: otherAccountUploadId,
                }))
                .catch(err => {
                    if (err.code !== 'NoSuchUpload') {
                        process.stdout.write('Error in other account ' +
                        `afterEach: ${err}\n`);
                        throw err;
                    }
                }).then(() => otherAccountBucketUtility
                .deleteOne(otherAccountBucket))
            );

            it('should not allow an account without read persmission on the ' +
                'source object to copy the object', done => {
                otherAccountS3.uploadPartCopy({ Bucket: otherAccountBucket,
                    Key: otherAccountKey,
                    CopySource: `${sourceBucketName}/${sourceObjName}`,
                    PartNumber: 1,
                    UploadId: otherAccountUploadId,
                },
                    err => {
                        checkError(err, 'AccessDenied');
                        done();
                    });
            });

            it('should not allow an account without write persmission on the ' +
                'destination bucket to upload part copy the object', done => {
                otherAccountS3.putObject({ Bucket: otherAccountBucket,
                    Key: otherAccountKey, Body: '' }, () => {
                    otherAccountS3.uploadPartCopy({ Bucket: destBucketName,
                        Key: destObjName,
                        CopySource: `${otherAccountBucket}/${otherAccountKey}`,
                        PartNumber: 1,
                        UploadId: uploadId,
                    },
                        err => {
                            checkError(err, 'AccessDenied');
                            done();
                        });
                });
            });

            it('should allow an account with read permission on the ' +
                'source object and write permission on the destination ' +
                'bucket to upload part copy the object', done => {
                s3.putObjectAcl({ Bucket: sourceBucketName,
                    Key: sourceObjName, ACL: 'public-read' }, () => {
                    otherAccountS3.uploadPartCopy({ Bucket: otherAccountBucket,
                        Key: otherAccountKey,
                        CopySource: `${sourceBucketName}/${sourceObjName}`,
                        PartNumber: 1,
                        UploadId: otherAccountUploadId,
                    },
                        err => {
                            checkNoError(err);
                            done();
                        });
                });
            });
        });
    });
});
