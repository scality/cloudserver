const assert = require('assert');
const { v4: uuidv4 } = require('uuid');
const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const async = require('async');
const { 
    CreateBucketCommand,
    CreateMultipartUploadCommand,
    UploadPartCommand,
    CompleteMultipartUploadCommand,
    AbortMultipartUploadCommand,
    GetObjectCommand,
    ListMultipartUploadsCommand,
    ListObjectVersionsCommand,
    DeleteObjectCommand,
    PutBucketVersioningCommand
} = require('@aws-sdk/client-s3');

const date = Date.now();
const bucket = `abortmpu${date}`;
const key = 'key';
const bodyFirstPart = Buffer.allocUnsafe(10).fill(0);

function checkError(err, code, message) {
    assert.notEqual(err, null, 'Expected failure but got success');
    assert.strictEqual(err.Code, code);
    assert.strictEqual(err.message, message);
}

describe('Abort MPU', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;
        let uploadId;

        beforeEach(async () => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            try {
                await s3.send(new CreateBucketCommand({ Bucket: bucket }));
                const mpu = await s3.send(new CreateMultipartUploadCommand({
                    Bucket: bucket,
                    Key: key,
                }));
                uploadId = mpu.UploadId;
                await s3.send(new UploadPartCommand({
                    Bucket: bucket, Key: key,
                    PartNumber: 1, UploadId: uploadId, Body: bodyFirstPart,
                }));
            } catch (err) {
                process.stdout.write(`Error in beforeEach: ${err}\n`);
                throw err;
            }
        });

        afterEach(async () => {
            await s3.send(new AbortMultipartUploadCommand({
                Bucket: bucket,
                Key: key,
                UploadId: uploadId,
            }));
            await bucketUtil.empty(bucket);
            await bucketUtil.deleteOne(bucket);
        });

        // aws-sdk now (v2.363.0) returns 'UriParameterError' error
        // this test was not replaced in any other suite
        it.skip('should return InvalidRequest error if aborting without key',
            done => {
                s3.send(new AbortMultipartUploadCommand({
                    Bucket: bucket,
                    Key: '',
                    UploadId: uploadId
                }))
                    .then(() => {
                        done(new Error('Expected failure but got success'));
                    })
                    .catch(err => {
                        checkError(err, 'InvalidRequest', 'A key must be specified');
                        done();
                    });
            });
    });
});

describe('Abort MPU with existing object', function AbortMPUExistingObject() {
    this.timeout(60000);

    withV4(sigCfg => {
        let bucketUtil;
        let s3;
        const bucketName = `abortmpu-test-bucket-${Date.now()}`;
        const objectKey = 'my-object';

        beforeEach(async () => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            await s3.send(new CreateBucketCommand({ Bucket: bucketName }));
        });

        afterEach(async () => {
            // Clean up all multipart uploads
            const listMPUResponse = await s3.send(new ListMultipartUploadsCommand({ Bucket: bucketName }));
            // eslint-disable-next-line no-console
            console.log('listMPUResponse', listMPUResponse);
            const uploads = listMPUResponse.Uploads || [];
            await Promise.all(uploads.map(async upload => {
                try {
                    await s3.send(new AbortMultipartUploadCommand({
                        Bucket: bucketName,
                        Key: upload.Key,
                        UploadId: upload.UploadId,
                    }));
                } catch (err) {
                    // eslint-disable-next-line no-console
                    console.log('Error aborting MPU', err);
                    if (err.name !== 'NoSuchUpload') {
                        throw err;
                    }
                }
            }));
            await bucketUtil.empty(bucketName);
            await bucketUtil.deleteOne(bucketName);
        });

        it('should not delete existing object data when aborting another MPU for same key', done => {
            const part1 = Buffer.from('I am part 1 of MPU 1');
            const part2 = Buffer.from('I am part 1 of MPU 2');
            let uploadId1;
            let uploadId2;
            let etag1;
            async.waterfall([
                next => {
                    s3.send(new CreateMultipartUploadCommand({
                        Bucket: bucketName,
                        Key: objectKey,
                    }))
                        .then(data => {
                            uploadId1 = data.UploadId;
                            return s3.send(new UploadPartCommand({
                                Bucket: bucketName,
                                Key: objectKey,
                                PartNumber: 1,
                                UploadId: uploadId1,
                                Body: part1,
                            }));
                        })
                        .then(data => {
                            etag1 = data.ETag;
                            return s3.send(new CompleteMultipartUploadCommand({
                                Bucket: bucketName,
                                Key: objectKey,
                                UploadId: uploadId1,
                                MultipartUpload: { Parts: [{ ETag: etag1, PartNumber: 1 }] },
                            }));
                        })
                        .then(() => next())
                        .catch(err => next(err));
                },
                next => {
                    s3.send(new GetObjectCommand({
                        Bucket: bucketName,
                        Key: objectKey,
                    }))
                        .then(async data => {
                            const bodyText = await data.Body.transformToString();
                            assert.strictEqual(bodyText, part1.toString());
                            next();
                        })
                        .catch(err => next(err));
                },
                next => {
                    s3.send(new CreateMultipartUploadCommand({
                        Bucket: bucketName,
                        Key: objectKey,
                    }))
                        .then(data => {
                            uploadId2 = data.UploadId;
                            return s3.send(new UploadPartCommand({
                                Bucket: bucketName,
                                Key: objectKey,
                                PartNumber: 1,
                                UploadId: uploadId2,
                                Body: part2,
                            }));
                        })
                        .then(() => next())
                        .catch(err => next(err));
                },
                next => {
                    s3.send(new AbortMultipartUploadCommand({
                        Bucket: bucketName,
                        Key: objectKey,
                        UploadId: uploadId2,
                    }))
                        .then(() => next())
                        .catch(err => next(err));
                },
                next => {
                    s3.send(new GetObjectCommand({
                        Bucket: bucketName,
                        Key: objectKey,
                    }))
                        .then(async data => {
                            const bodyText = await data.Body.transformToString();
                            assert.strictEqual(bodyText, part1.toString());
                            next();
                        })
                        .catch(err => next(err));
                },
            ], done);
        });

        it('should not delete existing object data when aborting an old MPU for same key', done => {
            const part1 = Buffer.from('I am part 1 of MPU 1');
            const part2 = Buffer.from('I am part 1 of MPU 2');
            let uploadId1;
            let uploadId2;
            let etag2;
            async.waterfall([
                next => {
                    s3.send(new CreateMultipartUploadCommand({
                        Bucket: bucketName,
                        Key: objectKey,
                    }))
                        .then(data => {
                            uploadId1 = data.UploadId;
                            return s3.send(new UploadPartCommand({
                                Bucket: bucketName,
                                Key: objectKey,
                                PartNumber: 1,
                                UploadId: uploadId1,
                                Body: part1,
                            }));
                        })
                        .then(() => next())
                        .catch(err => next(err));
                },
                next => {
                    s3.send(new CreateMultipartUploadCommand({
                        Bucket: bucketName,
                        Key: objectKey,
                    }))
                        .then(data => {
                            uploadId2 = data.UploadId;
                            return s3.send(new UploadPartCommand({
                                Bucket: bucketName,
                                Key: objectKey,
                                PartNumber: 1,
                                UploadId: uploadId2,
                                Body: part2,
                            }));
                        })
                        .then(data => {
                            etag2 = data.ETag;
                            return s3.send(new CompleteMultipartUploadCommand({
                                Bucket: bucketName,
                                Key: objectKey,
                                UploadId: uploadId2,
                                MultipartUpload: { Parts: [{ ETag: etag2, PartNumber: 1 }] },
                            }));
                        })
                        .then(() => next())
                        .catch(err => next(err));
                },
                next => {
                    s3.send(new GetObjectCommand({
                        Bucket: bucketName,
                        Key: objectKey,
                    }))
                        .then(async data => {
                            const bodyText = await data.Body.transformToString();
                            assert.strictEqual(bodyText, part2.toString());
                            next();
                        })
                        .catch(err => next(err));
                },
                next => {
                    s3.send(new AbortMultipartUploadCommand({
                        Bucket: bucketName,
                        Key: objectKey,
                        UploadId: uploadId1,
                    }))
                        .then(() => next())
                        .catch(err => next(err));
                },
                next => {
                    s3.send(new GetObjectCommand({
                        Bucket: bucketName,
                        Key: objectKey,
                    }))
                        .then(async data => {
                            const bodyText = await data.Body.transformToString();
                            assert.strictEqual(bodyText, part2.toString());
                            next();
                        })
                        .catch(err => next(err));
                },
            ], done);
        });
    });
});

describe('Abort MPU - No Such Upload', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        beforeEach(async () => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            await s3.send(new CreateBucketCommand({ Bucket: bucket }));
        });

        afterEach(() => bucketUtil.deleteOne(bucket));

        it('should return NoSuchUpload error when aborting non-existent mpu',
            done => {
                s3.send(new AbortMultipartUploadCommand({
                    Bucket: bucket,
                    Key: key,
                    UploadId: uuidv4().replace(/-/g, '')
                }))
                    .then(() => {
                        done(new Error('Expected failure but got success'));
                    })
                    .catch(err => {
                        assert.notEqual(err, null, 'Expected failure but got success');
                        assert.strictEqual(err.name, 'NoSuchUpload');
                        done();
                    });
            });
    });
});

describe('Abort MPU - Versioned Bucket Cleanup', function testSuite() {
    this.timeout(30000);

    withV4(sigCfg => {
        let bucketUtil;
        let s3;
        const bucketName = `abort-mpu-versioned-${Date.now()}`;
        const objectKey = 'test-object-with-versions';

        beforeEach(async () => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;

            await s3.send(new CreateBucketCommand({ Bucket: bucketName }));
            await s3.send(new PutBucketVersioningCommand({
                Bucket: bucketName,
                VersioningConfiguration: { Status: 'Enabled' },
            }));
        });

        afterEach(async () => {
            // Clean up all multipart uploads first
            const listMPUResponse = await s3.send(new ListMultipartUploadsCommand({ Bucket: bucketName }));
            if (listMPUResponse.Uploads && listMPUResponse.Uploads.length > 0) {
                await Promise.all(listMPUResponse.Uploads.map(async upload => {
                        await s3.send(new AbortMultipartUploadCommand({
                            Bucket: bucketName,
                            Key: upload.Key,
                            UploadId: upload.UploadId,
                        })).catch(err => {
                            if (err.name !== 'NoSuchUpload') {
                                throw err;
                            }
                        });
                }));
            }

            // Clean up all object versions
            const listVersionsResponse = await s3.send(new ListObjectVersionsCommand({ Bucket: bucketName }));
            const allObjects = [
                ...(listVersionsResponse.Versions || []),
                ...(listVersionsResponse.DeleteMarkers || [])
            ];
            await Promise.all(allObjects.map(async obj => {
                    await s3.send(new DeleteObjectCommand({
                        Bucket: bucketName,
                        Key: obj.Key,
                        VersionId: obj.VersionId,
                    }));
            }));
            await bucketUtil.deleteOne(bucketName);
        });

        it('should handle aborting MPU with many versions of same object', done => {
            const numberOfVersions = 5;
            let currentVersion = 0;
            let finalUploadId;

            // Create multiple versions of the same object
            async.whilst(
                () => currentVersion < numberOfVersions,
                callback => {
                    currentVersion++;
                    const data = Buffer.from(`Version ${currentVersion} data`);

                    async.waterfall([
                        next => {
                            s3.send(new CreateMultipartUploadCommand({
                                Bucket: bucketName,
                                Key: objectKey,
                            }))
                                .then(result => {
                                    if (currentVersion === numberOfVersions) {
                                        finalUploadId = result.UploadId; // Save the last one for aborting
                                    }
                                    next(null, result.UploadId);
                                })
                                .catch(err => next(err));
                        },
                        (uploadId, next) => {
                            s3.send(new UploadPartCommand({
                                Bucket: bucketName,
                                Key: objectKey,
                                PartNumber: 1,
                                UploadId: uploadId,
                                Body: data,
                            }))
                                .then(result => next(null, uploadId, result.ETag))
                                .catch(err => next(err));
                        },
                        (uploadId, etag, next) => {
                            if (currentVersion === numberOfVersions) {
                                // Don't complete the last one - we'll abort it
                                return next();
                            }

                            return s3.send(new CompleteMultipartUploadCommand({
                                Bucket: bucketName,
                                Key: objectKey,
                                UploadId: uploadId,
                                MultipartUpload: {
                                    Parts: [{ ETag: etag, PartNumber: 1 }],
                                },
                            }))
                                .then(() => next())
                                .catch(err => next(err));
                        },
                    ], callback);
                },
                err => {
                    assert.ifError(err);

                    // Now abort the final MPU
                    s3.send(new AbortMultipartUploadCommand({
                        Bucket: bucketName,
                        Key: objectKey,
                        UploadId: finalUploadId,
                    }))
                        .then(() => s3.send(new ListObjectVersionsCommand({ Bucket: bucketName })))
                        .then(data => {
                            const objectVersions = data.Versions.filter(v => v.Key === objectKey);
                            assert.strictEqual(objectVersions.length, numberOfVersions - 1,
                                `Expected ${numberOfVersions - 1} versions after abort, got ${objectVersions.length}`);
                            done();
                        })
                        .catch(err => done(err));
                }
            );
        });

        it('should handle abort MPU when object has no versions', done => {
            let uploadId;
            const data = Buffer.from('test data for single MPU abort');

            async.waterfall([
                // Create and upload part for MPU
                next => {
                    s3.send(new CreateMultipartUploadCommand({
                        Bucket: bucketName,
                        Key: objectKey,
                    }))
                        .then(result => {
                            uploadId = result.UploadId;
                            next();
                        })
                        .catch(err => next(err));
                },
                next => {
                    s3.send(new UploadPartCommand({
                        Bucket: bucketName,
                        Key: objectKey,
                        PartNumber: 1,
                        UploadId: uploadId,
                        Body: data,
                    }))
                        .then(() => next())
                        .catch(err => next(err));
                },

                // Abort the MPU
                next => {
                    s3.send(new AbortMultipartUploadCommand({
                        Bucket: bucketName,
                        Key: objectKey,
                        UploadId: uploadId,
                    }))
                        .then(() => next())
                        .catch(err => next(err));
                },

                // Verify no object exists
                next => {
                    s3.send(new GetObjectCommand({
                        Bucket: bucketName,
                        Key: objectKey,
                    }))
                        .then(() => {
                            next(new Error('Expected NoSuchKey error'));
                        })
                        .catch(err => {
                            assert.strictEqual(err.name, 'NoSuchKey');
                            next();
                        });
                },

                // Verify no versions exist
                next => {
                    s3.send(new ListObjectVersionsCommand({ Bucket: bucketName }))
                        .then(data => {
                            const objectVersions = data.Versions?.filter(v => v.Key === objectKey) || [];
                            assert.strictEqual(objectVersions.length, 0,
                                `Expected 0 versions after abort, got ${objectVersions.length}`);
                            next();
                        })
                        .catch(err => next(err));
                },
            ], done);
        });
    });
}); 
