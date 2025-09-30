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

const date = Date.now();
const bucket = `abortmpu${date}`;
const key = 'key';
const bodyFirstPart = Buffer.allocUnsafe(10).fill(0);

function checkError(err, code, message) {
    assert.notEqual(err, null, 'Expected failure but got success');
    assert.strictEqual(err.code, code);
    assert.strictEqual(err.message, message);
}

async function cleanupVersionedBucket(bucketUtil, bucketName) {
    // Clean up all multipart uploads
    const listMPUResponse = await bucketUtil.s3.listMultipartUploads({ Bucket: bucketName }).promise();
    await Promise.all(listMPUResponse.Uploads.map(upload =>
        bucketUtil.s3.abortMultipartUpload({
            Bucket: bucketName,
            Key: upload.Key,
            UploadId: upload.UploadId,
        }).promise().catch(err => {
            if (err.code !== 'NoSuchUpload') {
                throw err;
            }
            // If NoSuchUpload, swallow error
        }),
    ));

    // Clean up all object versions
    await bucketUtil.empty(bucketName);
    await bucketUtil.deleteOne(bucketName);
}

describe('Abort MPU', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;
        let uploadId;

        beforeEach(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return s3.createBucket({ Bucket: bucket }).promise()
                .then(() => s3.createMultipartUpload({
                    Bucket: bucket,
                    Key: key,
                }).promise())
                .then(res => {
                    uploadId = res.UploadId;
                    return s3.uploadPart({
                        Bucket: bucket, Key: key,
                        PartNumber: 1, UploadId: uploadId, Body: bodyFirstPart,
                    }).promise();
                })
                .catch(err => {
                    process.stdout.write(`Error in beforeEach: ${err}\n`);
                    throw err;
                });
        });

        afterEach(() =>
            s3.abortMultipartUpload({
                Bucket: bucket,
                Key: key,
                UploadId: uploadId,
            }).promise()
                .then(() => bucketUtil.empty(bucket))
                .then(() => bucketUtil.deleteOne(bucket))
        );

        // aws-sdk now (v2.363.0) returns 'UriParameterError' error
        // this test was not replaced in any other suite
        it.skip('should return InvalidRequest error if aborting without key',
            done => {
                s3.abortMultipartUpload({
                    Bucket: bucket,
                    Key: '',
                    UploadId: uploadId
                },
                    err => {
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

        beforeEach(done => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            s3.createBucket({ Bucket: bucketName }, err => {
                assert.ifError(err, `Error creating bucket: ${err}`);
                done();
            });
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
                    s3.createMultipartUpload({ Bucket: bucketName, Key: objectKey }, (err, data) => {
                        assert.ifError(err, `error creating MPU 1: ${err}`);
                        uploadId1 = data.UploadId;
                        s3.uploadPart({
                            Bucket: bucketName,
                            Key: objectKey,
                            PartNumber: 1,
                            UploadId: uploadId1,
                            Body: part1,
                        }, (err, data) => {
                            assert.ifError(err, `error uploading part for MPU 1: ${err}`);
                            etag1 = data.ETag;
                            s3.completeMultipartUpload({
                                Bucket: bucketName,
                                Key: objectKey,
                                UploadId: uploadId1,
                                MultipartUpload: { Parts: [{ ETag: etag1, PartNumber: 1 }] },
                            }, err => {
                                assert.ifError(err, `error completing MPU 1: ${err}`);
                                next();
                            });
                        });
                    });
                },
                next => {
                    s3.getObject({ Bucket: bucketName, Key: objectKey }, (err, data) => {
                        assert.ifError(err, `error getting object after MPU 1: ${err}`);
                        assert.strictEqual(data.Body.toString(), part1.toString());
                        next();
                    });
                },
                next => {
                    s3.createMultipartUpload({ Bucket: bucketName, Key: objectKey }, (err, data) => {
                        assert.ifError(err, `error creating MPU 2: ${err}`);
                        uploadId2 = data.UploadId;
                        s3.uploadPart({
                            Bucket: bucketName,
                            Key: objectKey,
                            PartNumber: 1,
                            UploadId: uploadId2,
                            Body: part2,
                        }, err => {
                            assert.ifError(err, `error uploading part for MPU 2: ${err}`);
                            next();
                        });
                    });
                },
                next => {
                    s3.abortMultipartUpload({ Bucket: bucketName, Key: objectKey, UploadId: uploadId2 }, err => {
                        assert.ifError(err, `error aborting MPU 2: ${err}`);
                        next();
                    });
                },
                next => {
                    s3.getObject({ Bucket: bucketName, Key: objectKey }, (err, data) => {
                        assert.ifError(err, `error getting object after aborting MPU 2: ${err}`);
                        assert.strictEqual(data.Body.toString(), part1.toString());
                        next();
                    });
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
                    s3.createMultipartUpload({
                        Bucket: bucketName, Key: objectKey,
                    }, (err, data) => {
                        assert.ifError(err, `error creating MPU 1: ${err}`);
                        uploadId1 = data.UploadId;
                        s3.uploadPart({
                            Bucket: bucketName,
                            Key: objectKey,
                            PartNumber: 1,
                            UploadId: uploadId1,
                            Body: part1,
                        }, err => {
                            assert.ifError(err, `error uploading part for MPU 1: ${err}`);
                            next();
                        });
                    });
                },
                next => {
                    s3.createMultipartUpload({
                        Bucket: bucketName, Key: objectKey,
                    }, (err, data) => {
                        assert.ifError(err, `error creating MPU 2: ${err}`);
                        uploadId2 = data.UploadId;
                        s3.uploadPart({
                            Bucket: bucketName,
                            Key: objectKey,
                            PartNumber: 1,
                            UploadId: uploadId2,
                            Body: part2,
                        }, (err, data) => {
                            assert.ifError(err, `error uploading part for MPU 2: ${err}`);
                            etag2 = data.ETag;
                            s3.completeMultipartUpload({
                                Bucket: bucketName,
                                Key: objectKey,
                                UploadId: uploadId2,
                                MultipartUpload: { Parts: [{ ETag: etag2, PartNumber: 1 }] },
                            }, err => {
                                assert.ifError(err, `error completing MPU 2: ${err}`);
                                next();
                            });
                        });
                    });
                },
                next => {
                    s3.getObject({
                        Bucket: bucketName,
                        Key: objectKey,
                    }, (err, data) => {
                        assert.ifError(err, `error getting object after MPU 2: ${err}`);
                        assert.strictEqual(data.Body.toString(), part2.toString());
                        next();
                    });
                },
                next => {
                    s3.abortMultipartUpload({
                        Bucket: bucketName,
                        Key: objectKey,
                        UploadId: uploadId1,
                    }, err => {
                        assert.ifError(err, `error aborting MPU 1: ${err}`);
                        next();
                    });
                },
                next => {
                    s3.getObject({ Bucket: bucketName, Key: objectKey }, (err, data) => {
                        assert.ifError(err, `error getting object after aborting MPU 1: ${err}`);
                        assert.strictEqual(data.Body.toString(), part2.toString());
                        next();
                    });
                },
            ], done);
        });
    });
});

describe('Abort MPU - No Such Upload', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        beforeEach(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return s3.createBucket({ Bucket: bucket }).promise();
        });

        afterEach(() => bucketUtil.deleteOne(bucket));

        it('should return NoSuchUpload error when aborting non-existent mpu',
            done => {
                s3.abortMultipartUpload({
                    Bucket: bucket,
                    Key: key,
                    UploadId: uuidv4().replace(/-/g, '')
                },
                    err => {
                        assert.notEqual(err, null, 'Expected failure but got success');
                        assert.strictEqual(err.code, 'NoSuchUpload');
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

        beforeEach(done => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;

            async.series([
                next => s3.createBucket({ Bucket: bucketName }, next),
                next => s3.putBucketVersioning({
                    Bucket: bucketName,
                    VersioningConfiguration: { Status: 'Enabled' },
                }, next),
            ], done);
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
                            s3.createMultipartUpload({
                                Bucket: bucketName,
                                Key: objectKey,
                            }, (err, result) => {
                                assert.ifError(err);
                                if (currentVersion === numberOfVersions) {
                                    finalUploadId = result.UploadId; // Save the last one for aborting
                                }
                                next(null, result.UploadId);
                            });
                        },
                        (uploadId, next) => {
                            s3.uploadPart({
                                Bucket: bucketName,
                                Key: objectKey,
                                PartNumber: 1,
                                UploadId: uploadId,
                                Body: data,
                            }, (err, result) => {
                                assert.ifError(err);
                                next(null, uploadId, result.ETag);
                            });
                        },
                        (uploadId, etag, next) => {
                            if (currentVersion === numberOfVersions) {
                                // Don't complete the last one - we'll abort it
                                return next();
                            }

                            return s3.completeMultipartUpload({
                                Bucket: bucketName,
                                Key: objectKey,
                                UploadId: uploadId,
                                MultipartUpload: {
                                    Parts: [{ ETag: etag, PartNumber: 1 }],
                                },
                            }, next);
                        },
                    ], callback);
                },
                err => {
                    assert.ifError(err);

                    // Now abort the final MPU
                    s3.abortMultipartUpload({
                        Bucket: bucketName,
                        Key: objectKey,
                        UploadId: finalUploadId,
                    }, err => {
                        assert.ifError(err);

                        // Verify we still have the correct number of completed versions
                        s3.listObjectVersions({ Bucket: bucketName }, (err, data) => {
                            assert.ifError(err);

                            const objectVersions = data.Versions.filter(v => v.Key === objectKey);
                            assert.strictEqual(objectVersions.length, numberOfVersions - 1,
                                `Expected ${numberOfVersions - 1} versions after abort, got ${objectVersions.length}`);

                            done();
                        });
                    });
                }
            );
        });

        it('should handle abort MPU when object has no versions', done => {
            let uploadId;
            const data = Buffer.from('test data for single MPU abort');

            async.waterfall([
                // Create and upload part for MPU
                next => {
                    s3.createMultipartUpload({
                        Bucket: bucketName,
                        Key: objectKey,
                    }, (err, result) => {
                        assert.ifError(err);
                        uploadId = result.UploadId;
                        next();
                    });
                },
                next => {
                    s3.uploadPart({
                        Bucket: bucketName,
                        Key: objectKey,
                        PartNumber: 1,
                        UploadId: uploadId,
                        Body: data,
                    }, err => {
                        assert.ifError(err);
                        next();
                    });
                },

                // Abort the MPU
                next => {
                    s3.abortMultipartUpload({
                        Bucket: bucketName,
                        Key: objectKey,
                        UploadId: uploadId,
                    }, err => {
                        assert.ifError(err);
                        next();
                    });
                },

                // Verify no object exists
                next => {
                    s3.getObject({ Bucket: bucketName, Key: objectKey }, err => {
                        assert.notEqual(err, null, 'Expected NoSuchKey error');
                        assert.strictEqual(err.code, 'NoSuchKey');
                        next();
                    });
                },

                // Verify no versions exist
                next => {
                    s3.listObjectVersions({ Bucket: bucketName }, (err, data) => {
                        assert.ifError(err);

                        const objectVersions = data.Versions.filter(v => v.Key === objectKey);
                        assert.strictEqual(objectVersions.length, 0,
                            `Expected 0 versions after abort, got ${objectVersions.length}`);

                        next();
                    });
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
        const createResult = await s3Client.createMultipartUpload({
            Bucket: bucketName,
            Key: tempObjectKey,
        }).promise();
        const tempUploadId = createResult.UploadId;

        const uploadResult = await s3Client.uploadPart({
            Bucket: bucketName,
            Key: tempObjectKey,
            PartNumber: 1,
            UploadId: tempUploadId,
            Body: data,
        }).promise();
        const tempEtag = uploadResult.ETag;

        const completeResult = await s3Client.completeMultipartUpload({
            Bucket: bucketName,
            Key: tempObjectKey,
            UploadId: tempUploadId,
            MultipartUpload: { Parts: [{ ETag: tempEtag, PartNumber: 1 }] },
        }).promise();

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

        await s3Client.deleteObject(deleteParams).promise();

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
                next => s3.createBucket({ Bucket: bucketName }, next),
                next => initMetadata(next),
            ], done);
        });

        afterEach(async () => {
            await cleanupVersionedBucket(bucketUtil, bucketName);
        });

        it('should detect and clean up orphaned object metadata created by failed CompleteMPU', async () => {
            const data = Buffer.from('test data for orphan cleanup');

            // Create MPU and upload a part
            const createResult = await s3.createMultipartUpload({
                Bucket: bucketName,
                Key: objectKey,
            }).promise();
            const uploadId = createResult.UploadId;

            await s3.uploadPart({
                Bucket: bucketName,
                Key: objectKey,
                PartNumber: 1,
                UploadId: uploadId,
                Body: data,
            }).promise();

            // Create realistic orphaned object metadata like a CompleteMPU would when failing before cleanup
            await createOrphanedObjectMetadata(s3, bucketName, objectKey, uploadId, data, false);

            // Verify the orphaned object exists
            await s3.headObject({ Bucket: bucketName, Key: objectKey }).promise();

            // Abort MPU - should clean up the orphaned object
            await s3.abortMultipartUpload({
                Bucket: bucketName,
                Key: objectKey,
                UploadId: uploadId,
            }).promise();

            // Verify the orphaned object was cleaned up
            try {
                await s3.headObject({ Bucket: bucketName, Key: objectKey }).promise();
                assert.fail('Orphaned object should be deleted after abort');
            } catch (err) {
                assert(err);
                assert.strictEqual(err.code, 'NotFound');
            }
        });

        it('should find and clean up orphaned object in versioned bucket', async () => {
            const data = Buffer.from('test versioned orphan cleanup');

            // Enable versioning
            await s3.putBucketVersioning({
                Bucket: bucketName,
                VersioningConfiguration: { Status: 'Enabled' },
            }).promise();

            // Create MPU
            const createResult = await s3.createMultipartUpload({
                Bucket: bucketName,
                Key: objectKey,
            }).promise();
            const uploadId = createResult.UploadId;

            await s3.uploadPart({
                Bucket: bucketName,
                Key: objectKey,
                PartNumber: 1,
                UploadId: uploadId,
                Body: data,
            }).promise();

            // Create realistic orphaned object metadata like a CompleteMPU would when failing before cleanup
            const orphanedObjectMD = await createOrphanedObjectMetadata(
                s3, bucketName, objectKey, uploadId, data, true);

            // Put a new master version on top of the orphaned version
            // The abort will fetch this during standardMetadataValidateBucketAndObj
            // It will force abort to findObjectVersionByUploadId
            await s3.putObject({
                Bucket: bucketName,
                Key: objectKey,
                Body: 'version 2 data',
            }).promise();

            // Verify we have 2 versions (1 regular + 1 orphaned)
            let listResult = await s3.listObjectVersions({ Bucket: bucketName }).promise();
            let objectVersions = listResult.Versions.filter(v => v.Key === objectKey);
            assert.strictEqual(objectVersions.length, 2,
                'Expected 2 versions before abort, 1 regular + 1 orphaned'
            );

            // Abort MPU - should find and clean up only the orphaned version
            await s3.abortMultipartUpload({
                Bucket: bucketName,
                Key: objectKey,
                UploadId: uploadId,
            }).promise();

            // Verify only the orphaned version was deleted
            listResult = await s3.listObjectVersions({ Bucket: bucketName }).promise();
            objectVersions = listResult.Versions.filter(v => v.Key === objectKey);
            assert.strictEqual(objectVersions.length, 1,
                'Should have 1 version after abort (orphaned version cleaned up)');

            // ensure orphanedObj doesn't exist
            try {
                await s3.headObject({ Bucket: bucketName, Key: objectKey,
                    VersionId: orphanedObjectMD.versionId }).promise();
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

            await s3.createBucket({ Bucket: bucketName }).promise();
        });

        afterEach(async () => {
            await cleanupVersionedBucket(bucketUtil, bucketName);
        });

        it('should handle abort during concurrent CompleteMPU without corruption', async () => {
            const data = Buffer.from('test concurrent complete and abort');

            // Create MPU and upload part
            const createResult = await s3.createMultipartUpload({
                Bucket: bucketName,
                Key: objectKey,
            }).promise();
            const uploadId = createResult.UploadId;

            const uploadResult = await s3.uploadPart({
                Bucket: bucketName,
                Key: objectKey,
                PartNumber: 1,
                UploadId: uploadId,
                Body: data,
            }).promise();
            const etag = uploadResult.ETag;

            // Start concurrent operations: CompleteMPU and AbortMPU
            const [completeResult, abortResult] = await Promise.allSettled([
                s3.completeMultipartUpload({
                    Bucket: bucketName,
                    Key: objectKey,
                    UploadId: uploadId,
                    MultipartUpload: {
                        Parts: [{ ETag: etag, PartNumber: 1 }],
                    },
                }).promise(),

                // Add small delay to create race condition
                scheduler.wait(10).then(() => s3.abortMultipartUpload({
                    Bucket: bucketName,
                    Key: objectKey,
                    UploadId: uploadId,
                }).promise())
            ]);

            // Verify final state is consistent
            const completeError = completeResult.status === 'rejected' ? completeResult.reason : null;
            const abortError = abortResult.status === 'rejected' ? abortResult.reason : null;

            if (!completeError) {
                // Complete succeeded - object should exist or be cleaned up
                try {
                    const headResult = await s3.headObject({ Bucket: bucketName, Key: objectKey }).promise();
                    // Complete won the race - verify object exists and is accessible
                    assert.ok(headResult.ETag, 'Object should have valid ETag');
                } catch (err) {
                    if (err.code === 'NotFound') {
                        // Abort may have cleaned up the object after complete created it
                        // This is acceptable
                    } else {
                        throw err;
                    }
                }
            } else if (!abortError) {
                // Abort succeeded - check if object exists or was cleaned up
                try {
                    await s3.headObject({ Bucket: bucketName, Key: objectKey }).promise();
                    // Either object exists (complete won) or doesn't (abort won)
                    // Both states are acceptable
                } catch {
                    // Object doesn't exist - this is also acceptable
                }
            }
            // If both operations encountered errors, that's also acceptable
            // as long as the system remains consistent

            // Verify no MPU metadata remains
            const listResult = await s3.listMultipartUploads({ Bucket: bucketName }).promise();
            const remainingUploads = listResult.Uploads.filter(upload => upload.UploadId === uploadId);
            assert.strictEqual(remainingUploads.length, 0, 'No MPU metadata should remain');
        });

        it('should handle multiple concurrent aborts on same MPU gracefully', async () => {
            const data = Buffer.from('test multiple concurrent aborts');

            // Create MPU and upload part
            const createResult = await s3.createMultipartUpload({
                Bucket: bucketName,
                Key: objectKey,
            }).promise();
            const uploadId = createResult.UploadId;

            await s3.uploadPart({
                Bucket: bucketName,
                Key: objectKey,
                PartNumber: 1,
                UploadId: uploadId,
                Body: data,
            }).promise();

            // Launch 3 concurrent abort operations
            const abortResults = await Promise.allSettled([
                s3.abortMultipartUpload({
                    Bucket: bucketName,
                    Key: objectKey,
                    UploadId: uploadId,
                }).promise(),
                s3.abortMultipartUpload({
                    Bucket: bucketName,
                    Key: objectKey,
                    UploadId: uploadId,
                }).promise(),
                s3.abortMultipartUpload({
                    Bucket: bucketName,
                    Key: objectKey,
                    UploadId: uploadId,
                }).promise()
            ]);

            // Verify results
            const abortErrors = abortResults.map(result =>
                result.status === 'rejected' ? result.reason : null
            );

            // At least one abort should succeed
            const successfulAborts = abortErrors.filter(err => !err);
            assert(successfulAborts.length >= 1, 'At least one abort should succeed');

            // Other aborts may fail with NoSuchUpload - this is acceptable
            const otherErrors = abortErrors.filter(err => err && err.code !== 'NoSuchUpload');
            assert.strictEqual(otherErrors.length, 0, 'Should not have unexpected errors');

            // Verify final cleanup state
            // No object should exist since no CompleteMPU was performed
            try {
                await s3.headObject({ Bucket: bucketName, Key: objectKey }).promise();
                assert.fail('No object should exist after aborting MPU');
            } catch (err) {
                if (err.code === 'NotFound') {
                    // Expected - no object should exist
                } else {
                    throw err;
                }
            }

            // Verify no MPU metadata remains
            const listResult = await s3.listMultipartUploads({ Bucket: bucketName }).promise();
            const remainingUploads = listResult.Uploads.filter(upload => upload.UploadId === uploadId);
            assert.strictEqual(remainingUploads.length, 0,
                'No MPU metadata should remain after concurrent aborts');
        });
    });
});
