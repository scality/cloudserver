const assert = require('assert');
const { v4: uuidv4 } = require('uuid');
const { promisify } = require('util');
const { scheduler } = require('timers/promises');
const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const async = require('async');
const { initMetadata, getMetadata } = require('../utils/init');
const metadata = require('../../../../../lib/metadata/wrapper');
const { DummyRequestLogger } = require('../../../../unit/helpers');
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
    PutBucketVersioningCommand,
    HeadObjectCommand,
    PutObjectCommand
} = require('@aws-sdk/client-s3');

const date = Date.now();
const bucket = `abortmpu${date}`;
const key = 'key';
const bodyFirstPart = Buffer.allocUnsafe(10).fill(0);

function checkError(err, code, message) {
    assert.notEqual(err, null, 'Expected failure but got success');
    assert.strictEqual(err.name, code);
    assert.strictEqual(err.message, message);
}

async function cleanupVersionedBucket(bucketUtil, bucketName) {
    // Clean up all multipart uploads
    const listMPUResponse = await bucketUtil.s3.send(new ListMultipartUploadsCommand({ Bucket: bucketName }));
    if (listMPUResponse.Uploads && listMPUResponse.Uploads.length > 0) {
        await Promise.all(listMPUResponse.Uploads.map(async upload => {
                bucketUtil.s3.send(new AbortMultipartUploadCommand({
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
    await bucketUtil.empty(bucketName);
    await bucketUtil.deleteOne(bucketName);
}

// Poll ListMultipartUploads until the given uploadId no longer appears.
// After a racing Complete/Abort, MPU metadata cleanup can be eventually
// consistent, so a single immediate read may still observe the upload and
// fail spuriously (CLDSRV-938). On timeout this returns the still-present
// uploads so the caller's assertion fails with the original message, which
// preserves detection of a genuine orphan-MPU leak.
async function waitForMpuCleanup(s3, bucketName, uploadId, { timeoutMs = 10000, intervalMs = 250 } = {}) {
    const deadline = Date.now() + timeoutMs;
    let remainingUploads = [];
    do {
        const listResult = await s3.send(new ListMultipartUploadsCommand({ Bucket: bucketName }));
        remainingUploads = (listResult.Uploads || []).filter(upload => upload.UploadId === uploadId);
        if (remainingUploads.length === 0) {
            return remainingUploads;
        }
        await scheduler.wait(intervalMs);
    } while (Date.now() < deadline);
    return remainingUploads;
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
            await cleanupVersionedBucket(bucketUtil, bucketName);
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
            await cleanupVersionedBucket(bucketUtil, bucketName);
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
                            const objectVersions = (data.Versions || []).filter(v => v.Key === objectKey);
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

describe('Abort MPU - Orphan Cleanup', function testSuite() {
    this.timeout(60000);
    const log = new DummyRequestLogger();

    // Promisify callback-based functions
    const getMetadataAsync = promisify(getMetadata);
    const putObjectMDAsync = promisify(metadata.putObjectMD.bind(metadata));

    /**
     * Helper function to create realistic orphaned object metadata
     * @param {Object} s3Client - S3 client instance
     * @param {string} bucketName - Target bucket name
     * @param {string} objectKey - Target object key for orphaned metadata
     * @param {string} uploadIdToSimulate - uploadId to assign to the orphaned object
     * @param {Buffer} data - Data to use for temporary MPU
     * @param {boolean} isVersioned - Whether to create versioned metadata
     * @returns {Promise} Promise that resolves when orphaned metadata is created
     */
    async function createOrphanedObjectMetadata(s3Client, bucketName, objectKey, uploadIdToSimulate,
        data, isVersioned) {
        const tempObjectKey = `temp-object-for-metadata-${Date.now()}`;

        // Create temporary MPU and complete it to get real object metadata
        const createResult = await s3Client.send(new CreateMultipartUploadCommand({
            Bucket: bucketName,
            Key: tempObjectKey,
        }));
        const tempUploadId = createResult.UploadId;

        const uploadResult = await s3Client.send(new UploadPartCommand({
            Bucket: bucketName,
            Key: tempObjectKey,
            PartNumber: 1,
            UploadId: tempUploadId,
            Body: data,
        }));
        const tempEtag = uploadResult.ETag;

        const completeResult = await s3Client.send(new CompleteMultipartUploadCommand({
            Bucket: bucketName,
            Key: tempObjectKey,
            UploadId: tempUploadId,
            MultipartUpload: { Parts: [{ ETag: tempEtag, PartNumber: 1 }] },
        }));

        let tempVersionId;
        if (isVersioned && completeResult.VersionId) {
            tempVersionId = completeResult.VersionId;
        }

        // Get the real object metadata
        const versionId = isVersioned ? tempVersionId : null;
        const objMD = await getMetadataAsync(bucketName, tempObjectKey, versionId);

        // Create a copy and override uploadId to match our test MPU
        // (simulating orphaned object)
        const orphanedObjectMD = Object.assign({}, objMD,
            // let metadata generate a new versionId
            { uploadId: uploadIdToSimulate, versionId: undefined });

        // Store this modified metadata as orphaned object
        const putOptions = isVersioned && objMD.versionId
            ? { versioning: true }
            : {};
        await putObjectMDAsync(bucketName, objectKey, orphanedObjectMD, putOptions, log);

        // Clean up temporary object
        const deleteParams = { Bucket: bucketName, Key: tempObjectKey };
        if (isVersioned && tempVersionId) {
            deleteParams.VersionId = tempVersionId;
        }

        await s3Client.send(new DeleteObjectCommand(deleteParams));

        return orphanedObjectMD;
    }

    withV4(sigCfg => {
        let bucketUtil;
        let s3;
        const bucketName = `abort-mpu-orphan-${Date.now()}`;
        const objectKey = 'test-object-with-orphans';

        beforeEach(done => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;

            async.series([
                next => s3.send(new CreateBucketCommand({ Bucket: bucketName })).then(() => 
                    next()).catch(err => next(err)),
                next => initMetadata(next),
            ], done);
        });

        afterEach(async () => {
            await cleanupVersionedBucket(bucketUtil, bucketName);
        });

        it('should detect and clean up orphaned object metadata created by failed CompleteMPU', async () => {
            const data = Buffer.from('test data for orphan cleanup');

            // Create MPU and upload a part
            const createResult = await s3.send(new CreateMultipartUploadCommand({
                Bucket: bucketName,
                Key: objectKey,
            }));
            const uploadId = createResult.UploadId;

            await s3.send(new UploadPartCommand({
                Bucket: bucketName,
                Key: objectKey,
                PartNumber: 1,
                UploadId: uploadId,
                Body: data,
            }));

            // Create realistic orphaned object metadata like a CompleteMPU would when failing before cleanup
            await createOrphanedObjectMetadata(s3, bucketName, objectKey, uploadId, data, false);

            // Verify the orphaned object exists
            await s3.send(new HeadObjectCommand({ Bucket: bucketName, Key: objectKey }));

            // Abort MPU - should clean up the orphaned object
            await s3.send(new AbortMultipartUploadCommand({
                Bucket: bucketName,
                Key: objectKey,
                UploadId: uploadId,
            }));

            // Verify the orphaned object was cleaned up
            try {
                await s3.send(new HeadObjectCommand({ Bucket: bucketName, Key: objectKey }));
                assert.fail('Orphaned object should be deleted after abort');
            } catch (err) {
                assert(err);
                assert.strictEqual(err.name, 'NotFound');
                assert.strictEqual(err.$metadata.httpStatusCode, 404);
            }
        });

        it('should find and clean up orphaned object in versioned bucket', async () => {
            const data = Buffer.from('test versioned orphan cleanup');

            // Enable versioning
            await s3.send(new PutBucketVersioningCommand({
                Bucket: bucketName,
                VersioningConfiguration: { Status: 'Enabled' },
            }));

            // Create MPU
            const createResult = await s3.send(new CreateMultipartUploadCommand({
                Bucket: bucketName,
                Key: objectKey,
            }));
            const uploadId = createResult.UploadId;

            await s3.send(new UploadPartCommand({
                Bucket: bucketName,
                Key: objectKey,
                PartNumber: 1,
                UploadId: uploadId,
                Body: data,
            }));

            // Create realistic orphaned object metadata like a CompleteMPU would when failing before cleanup
            const orphanedObjectMD = await createOrphanedObjectMetadata(
                s3, bucketName, objectKey, uploadId, data, true);

            // Put a new master version on top of the orphaned version
            // The abort will fetch this during standardMetadataValidateBucketAndObj
            // It will force abort to findObjectVersionByUploadId
            await s3.send(new PutObjectCommand({
                Bucket: bucketName,
                Key: objectKey,
                Body: 'version 2 data',
            }));

            // Verify we have 2 versions (1 regular + 1 orphaned)
            let listResult = await s3.send(new ListObjectVersionsCommand({ Bucket: bucketName }));
            let objectVersions = listResult.Versions.filter(v => v.Key === objectKey);
            assert.strictEqual(objectVersions.length, 2,
                'Expected 2 versions before abort, 1 regular + 1 orphaned'
            );

            // Abort MPU - should find and clean up only the orphaned version
            await s3.send(new AbortMultipartUploadCommand({
                Bucket: bucketName,
                Key: objectKey,
                UploadId: uploadId,
            }));

            // Verify only the orphaned version was deleted
            listResult = await s3.send(new ListObjectVersionsCommand({ Bucket: bucketName }));
            objectVersions = listResult.Versions.filter(v => v.Key === objectKey);
            assert.strictEqual(objectVersions.length, 1,
                'Should have 1 version after abort (orphaned version cleaned up)');

            // ensure orphanedObj doesn't exist
            try {
                await s3.send(new HeadObjectCommand({ Bucket: bucketName, Key: objectKey,
                    VersionId: orphanedObjectMD.versionId }));
                assert.fail('Orphaned object should be deleted after abort');
            } catch (err) {
                assert(err);
            }
        });
    });
});

describe('Abort MPU - Race Conditions', function testSuite() {
    this.timeout(60000);

    withV4(sigCfg => {
        let bucketUtil;
        let s3;
        const bucketName = `abort-mpu-race-${Date.now()}`;
        const objectKey = 'test-object-race-conditions';

        beforeEach(async () => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;

            await s3.send(new CreateBucketCommand({ Bucket: bucketName }));
        });

        afterEach(async () => {
            await cleanupVersionedBucket(bucketUtil, bucketName);
        });

        it('should handle abort during concurrent CompleteMPU without corruption', async () => {
            const data = Buffer.from('test concurrent complete and abort');

            // Create MPU and upload part
            const createResult = await s3.send(new CreateMultipartUploadCommand({
                Bucket: bucketName,
                Key: objectKey,
            }));
            const uploadId = createResult.UploadId;

            const uploadResult = await s3.send(new UploadPartCommand({
                Bucket: bucketName,
                Key: objectKey,
                PartNumber: 1,
                UploadId: uploadId,
                Body: data,
            }));
            const etag = uploadResult.ETag;

            // Start concurrent operations: CompleteMPU and AbortMPU
            const [completeResult, abortResult] = await Promise.allSettled([
                s3.send(new CompleteMultipartUploadCommand({
                    Bucket: bucketName,
                    Key: objectKey,
                    UploadId: uploadId,
                    MultipartUpload: {
                        Parts: [{ ETag: etag, PartNumber: 1 }],
                    },
                })),

                // Add small delay to create race condition
                scheduler.wait(10).then(() => s3.send(new AbortMultipartUploadCommand({
                    Bucket: bucketName,
                    Key: objectKey,
                    UploadId: uploadId,
                })))
            ]);

            // Verify final state is consistent
            const completeError = completeResult.status === 'rejected' ? completeResult.reason : null;
            const abortError = abortResult.status === 'rejected' ? abortResult.reason : null;

            if (!completeError) {
                // Complete succeeded - object should exist or be cleaned up
                try {
                    const headResult = await s3.send(new HeadObjectCommand({ Bucket: bucketName, Key: objectKey }));
                    // Complete won the race - verify object exists and is accessible
                    assert.ok(headResult.ETag, 'Object should have valid ETag');
                } catch (err) {
                    if (err.name === 'NotFound') {
                        // Abort may have cleaned up the object after complete created it
                        // This is acceptable
                    } else {
                        throw err;
                    }
                }
            } else if (!abortError) {
                // Abort succeeded - check if object exists or was cleaned up
                try {
                    await s3.send(new HeadObjectCommand({ Bucket: bucketName, Key: objectKey }));
                    // Either object exists (complete won) or doesn't (abort won)
                    // Both states are acceptable
                } catch {
                    // Object doesn't exist - this is also acceptable
                }
            }
            // If both operations encountered errors, that's also acceptable
            // as long as the system remains consistent

            // Verify no MPU metadata remains. Cleanup after a racing
            // Complete/Abort can be eventually consistent, so poll rather than
            // reading once (CLDSRV-938).
            const remainingUploads = await waitForMpuCleanup(s3, bucketName, uploadId);
            assert.strictEqual(remainingUploads.length, 0, 'No MPU metadata should remain');
        });

        it('should handle multiple concurrent aborts on same MPU gracefully', async () => {
            const data = Buffer.from('test multiple concurrent aborts');

            // Create MPU and upload part
            const createResult = await s3.send(new CreateMultipartUploadCommand({
                Bucket: bucketName,
                Key: objectKey,
            }));
            const uploadId = createResult.UploadId;

            await s3.send(new UploadPartCommand({
                Bucket: bucketName,
                Key: objectKey,
                PartNumber: 1,
                UploadId: uploadId,
                Body: data,
            }));

            // Launch 3 concurrent abort operations
            const abortResults = await Promise.allSettled([
                s3.send(new AbortMultipartUploadCommand({
                    Bucket: bucketName,
                    Key: objectKey,
                    UploadId: uploadId,
                })),
                s3.send(new AbortMultipartUploadCommand({
                    Bucket: bucketName,
                    Key: objectKey,
                    UploadId: uploadId,
                })),
                s3.send(new AbortMultipartUploadCommand({
                    Bucket: bucketName,
                    Key: objectKey,
                    UploadId: uploadId,
                }))
            ]);

            // Verify results
            const abortErrors = abortResults.map(result =>
                result.status === 'rejected' ? result.reason : null
            );

            // At least one abort should succeed
            const successfulAborts = abortErrors.filter(err => !err);
            assert(successfulAborts.length >= 1, 'At least one abort should succeed');

            // Other aborts may fail with NoSuchUpload - this is acceptable
            const otherErrors = abortErrors.filter(err => err && err.name !== 'NoSuchUpload');
            assert.strictEqual(otherErrors.length, 0, 'Should not have unexpected errors');

            // Verify final cleanup state
            // No object should exist since no CompleteMPU was performed
            try {
                await s3.send(new HeadObjectCommand({ Bucket: bucketName, Key: objectKey }));
                assert.fail('No object should exist after aborting MPU');
            } catch (err) {
                if (err.name === 'NotFound') {
                    // Expected - no object should exist
                } else {
                    throw err;
                }
            }

            // Verify no MPU metadata remains. Cleanup after concurrent aborts
            // can be eventually consistent, so poll rather than reading once
            // (CLDSRV-938).
            const remainingUploads = await waitForMpuCleanup(s3, bucketName, uploadId);
            assert.strictEqual(remainingUploads.length, 0,
                'No MPU metadata should remain after concurrent aborts');
        });
    });
});
