const async = require('async');
const assert = require('assert');
const getConfig = require('../functional/aws-node-sdk/test/support/config');
const { Scuba: MockScuba, inflightFlushFrequencyMS } = require('../utilities/mock/Scuba');
const sendRequest = require('../functional/aws-node-sdk/test/quota/tooling').sendRequest;
const memCredentials = require('../functional/aws-node-sdk/lib/json/mem_credentials.json');
const metadata = require('../../lib/metadata/wrapper');
const { fakeMetadataArchive } = require('../functional/aws-node-sdk/test/utils/init');
const { config: s3Config } = require('../../lib/Config');
const {
    S3Client,
    PutObjectCommand,
    CreateMultipartUploadCommand,
    UploadPartCommand,
    UploadPartCopyCommand,
    CompleteMultipartUploadCommand,
    PutBucketVersioningCommand,
    PutObjectLockConfigurationCommand,
    CopyObjectCommand,
    DeleteObjectCommand,
    DeleteObjectsCommand,
    AbortMultipartUploadCommand,
    RestoreObjectCommand,
    CreateBucketCommand,
    DeleteBucketCommand,
} = require('@aws-sdk/client-s3');

let mockScuba = null;
let s3Client = null;
const quota = { quota: 1000 };

function wait(timeoutMs, cb) {
    if (s3Config.isQuotaInflightEnabled()) {
        return setTimeout(cb, timeoutMs);
    }
    return cb();
}

function createBucket(bucket, locked, cb) {
    const config = {
        Bucket: bucket,
    };
    if (locked) {
        config.ObjectLockEnabledForBucket = true;
    }
    return s3Client
        .send(new CreateBucketCommand(config))
        .then(data => cb(null, data))
        .catch(cb);
}

function configureBucketVersioning(bucket, cb) {
    return s3Client
        .send(
            new PutBucketVersioningCommand({
                Bucket: bucket,
                VersioningConfiguration: {
                    Status: 'Enabled',
                },
            }),
        )
        .then(data => cb(null, data))
        .catch(cb);
}

function putObjectLockConfiguration(bucket, cb) {
    return s3Client
        .send(
            new PutObjectLockConfigurationCommand({
                Bucket: bucket,
                ObjectLockConfiguration: {
                    ObjectLockEnabled: 'Enabled',
                    Rule: {
                        DefaultRetention: {
                            Mode: 'GOVERNANCE',
                            Days: 1,
                        },
                    },
                },
            }),
        )
        .then(data => cb(null, data))
        .catch(cb);
}

function deleteBucket(bucket, cb) {
    return s3Client
        .send(new DeleteBucketCommand({ Bucket: bucket }))
        .then(data => cb(null, data))
        .catch(cb);
}

function putObject(bucket, key, size, cb) {
    return s3Client
        .send(
            new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: Buffer.alloc(size),
            }),
        )
        .then(data => {
            if (!s3Config.isQuotaInflightEnabled()) {
                mockScuba.incrementBytesForBucket(bucket, size);
            }
            return cb(null, data);
        })
        .catch(cb);
}

function putObjectWithCustomHeader(bucket, key, size, vID, cb) {
    const params = {
        Bucket: bucket,
        Key: key,
        Body: Buffer.alloc(size),
    };

    const command = new PutObjectCommand(params);
    command.middlewareStack.add(
        next => async args => {
            // eslint-disable-next-line no-param-reassign
            args.request.headers['x-scal-s3-version-id'] = vID;
            return next(args);
        },
        { step: 'build' },
    );

    return s3Client
        .send(command)
        .then(data => {
            if (!s3Config.isQuotaInflightEnabled()) {
                mockScuba.incrementBytesForBucket(bucket, 0);
            }
            cb(null, data);
        })
        .catch(cb);
}

function copyObject(bucket, key, sourceSize, cb) {
    return s3Client
        .send(
            new CopyObjectCommand({
                Bucket: bucket,
                CopySource: `${bucket}/${key}`,
                Key: `${key}-copy`,
            }),
        )
        .then(data => {
            if (!s3Config.isQuotaInflightEnabled()) {
                mockScuba.incrementBytesForBucket(bucket, sourceSize);
            }
            return cb(null, data);
        })
        .catch(cb);
}

function deleteObject(bucket, key, size, cb) {
    return s3Client
        .send(
            new DeleteObjectCommand({
                Bucket: bucket,
                Key: key,
            }),
        )
        .then(() => {
            if (!s3Config.isQuotaInflightEnabled()) {
                mockScuba.incrementBytesForBucket(bucket, -size);
            }
            return cb();
        })
        .catch(cb);
}

function deleteVersionID(bucket, key, versionId, size, cb) {
    return s3Client
        .send(
            new DeleteObjectCommand({
                Bucket: bucket,
                Key: key,
                VersionId: versionId,
            }),
        )
        .then(data => {
            if (!s3Config.isQuotaInflightEnabled()) {
                mockScuba.incrementBytesForBucket(bucket, -size);
            }
            return cb(null, data);
        })
        .catch(cb);
}

function objectMPU(bucket, key, parts, partSize, callback) {
    let ETags = [];
    let uploadId = null;
    const partNumbers = Array.from(Array(parts).keys());
    const initiateMPUParams = {
        Bucket: bucket,
        Key: key,
    };
    return async.waterfall(
        [
            next =>
                s3Client
                    .send(new CreateMultipartUploadCommand(initiateMPUParams))
                    .then(data => {
                        uploadId = data.UploadId;
                        return next();
                    })
                    .catch(next),
            next =>
                async.mapLimit(
                    partNumbers,
                    1,
                    (partNumber, callback) => {
                        const uploadPartParams = {
                            Bucket: bucket,
                            Key: key,
                            PartNumber: partNumber + 1,
                            UploadId: uploadId,
                            Body: Buffer.alloc(partSize),
                        };
                        return s3Client
                            .send(new UploadPartCommand(uploadPartParams))
                            .then(data => callback(null, data.ETag))
                            .catch(callback);
                    },
                    (err, results) => {
                        if (err) {
                            return next(err);
                        }
                        ETags = results;
                        return next();
                    },
                ),
            next => {
                const params = {
                    Bucket: bucket,
                    Key: key,
                    MultipartUpload: {
                        Parts: partNumbers.map(n => ({
                            ETag: ETags[n],
                            PartNumber: n + 1,
                        })),
                    },
                    UploadId: uploadId,
                };
                return s3Client
                    .send(new CompleteMultipartUploadCommand(params))
                    .then(data => next(null, data))
                    .catch(next);
            },
        ],
        err => {
            if (!err && !s3Config.isQuotaInflightEnabled()) {
                mockScuba.incrementBytesForBucket(bucket, parts * partSize);
            }
            return callback(err, uploadId);
        },
    );
}

function abortMPU(bucket, key, uploadId, size, callback) {
    return s3Client
        .send(
            new AbortMultipartUploadCommand({
                Bucket: bucket,
                Key: key,
                UploadId: uploadId,
            }),
        )
        .then(data => {
            if (!s3Config.isQuotaInflightEnabled()) {
                mockScuba.incrementBytesForBucket(bucket, -size);
            }
            return callback(null, data);
        })
        .catch(err => callback(err));
}

function uploadPartCopy(bucket, key, partNumber, partSize, sleepDuration, keyToCopy, callback) {
    const ETags = [];
    let uploadId = null;
    const parts = 5;
    const partNumbers = Array.from(Array(parts).keys());
    const initiateMPUParams = {
        Bucket: bucket,
        Key: key,
    };
    if (!s3Config.isQuotaInflightEnabled()) {
        mockScuba.incrementBytesForBucket(bucket, parts * partSize);
    }
    return async.waterfall(
        [
            next =>
                s3Client
                    .send(new CreateMultipartUploadCommand(initiateMPUParams))
                    .then(data => {
                        uploadId = data.UploadId;
                        return next();
                    })
                    .catch(next),
            next => {
                const uploadPartParams = {
                    Bucket: bucket,
                    Key: key,
                    PartNumber: partNumber + 1,
                    UploadId: uploadId,
                    Body: Buffer.alloc(partSize),
                };
                return s3Client
                    .send(new UploadPartCommand(uploadPartParams))
                    .then(data => {
                        ETags[partNumber] = data.ETag;
                        return next();
                    })
                    .catch(next);
            },
            next => wait(sleepDuration, next),
            next => {
                const copyPartParams = {
                    Bucket: bucket,
                    CopySource: `${bucket}/${keyToCopy}`,
                    Key: `${key}-copy`,
                    PartNumber: partNumber + 1,
                    UploadId: uploadId,
                };
                return s3Client
                    .send(new UploadPartCopyCommand(copyPartParams))
                    .then(data => {
                        ETags[partNumber] = data.CopyPartResult.ETag;
                        return next(null, data.CopyPartResult.ETag);
                    })
                    .catch(next);
            },
            next => {
                const params = {
                    Bucket: bucket,
                    Key: key,
                    MultipartUpload: {
                        Parts: partNumbers.map(n => ({
                            ETag: ETags[n],
                            PartNumber: n + 1,
                        })),
                    },
                    UploadId: uploadId,
                };
                return s3Client
                    .send(new CompleteMultipartUploadCommand(params))
                    .then(() => next())
                    .catch(next);
            },
        ],
        err => {
            if (err && !s3Config.isQuotaInflightEnabled()) {
                mockScuba.incrementBytesForBucket(bucket, -(parts * partSize));
            }
            return callback(err, uploadId);
        },
    );
}

function restoreObject(bucket, key, size, callback) {
    return s3Client
        .send(
            new RestoreObjectCommand({
                Bucket: bucket,
                Key: key,
                RestoreRequest: {
                    Days: 1,
                },
            }),
        )
        .then(data => {
            if (!s3Config.isQuotaInflightEnabled()) {
                mockScuba.incrementBytesForBucket(bucket, size);
            }
            return callback(null, data);
        })
        .catch(callback);
}

function multiObjectDelete(bucket, keys, size, callback) {
    if (!s3Config.isQuotaInflightEnabled()) {
        mockScuba.incrementBytesForBucket(bucket, -size);
    }
    const deleteObjectsParams = keys.map(key => ({ Key: key }));
    const command = new DeleteObjectsCommand({
        Bucket: bucket,
        Delete: {
            Objects: deleteObjectsParams,
            Quiet: false,
        },
    });

    return s3Client
        .send(command)
        .then(data => {
            callback(null, data);
        })
        .catch(err => {
            if (!s3Config.isQuotaInflightEnabled()) {
                mockScuba.incrementBytesForBucket(bucket, size);
            }
            return callback(err);
        });
}

(process.env.S3METADATA === 'mongodb' ? describe : describe.skip)('quota evaluation with scuba metrics', function t() {
    this.timeout(30000);
    const scuba = new MockScuba();
    const putQuotaVerb = 'PUT';
    const config = {
        accessKey: memCredentials.default.accessKey,
        secretKey: memCredentials.default.secretKey,
    };
    mockScuba = scuba;

    before(done => {
        const config = getConfig('default', {
            maxRetries: 0,
        });

        s3Client = new S3Client({
            ...config,
            // Disable ALL automatic checksum handling
            requestChecksumCalculation: 'WHEN_REQUIRED',
            responseChecksumValidation: 'WHEN_REQUIRED',
            checksumDisabled: true,
            disableRequestCompression: true,
            // Force the client to not add automatic headers
            useGlobalEndpoint: false,
        });

        scuba.start();
        metadata.setup(err => wait(2000, () => done(err)));
    });

    afterEach(() => {
        scuba.reset();
    });

    after(() => {
        scuba.stop();
    });

    it('should return QuotaExceeded when trying to PutObject in a bucket with quota', done => {
        const bucket = 'quota-test-bucket1';
        const key = 'quota-test-object';
        const size = 1024;
        return async.series(
            [
                next => createBucket(bucket, false, next),
                next =>
                    sendRequest(putQuotaVerb, '127.0.0.1:8000', `/${bucket}/?quota=true`, JSON.stringify(quota), config)
                        .then(() => next())
                        .catch(err => next(err)),
                next => {
                    putObject(bucket, key, size, err => {
                        try {
                            assert.strictEqual(err.name, 'QuotaExceeded');
                            return next();
                        } catch (assertError) {
                            return next(assertError);
                        }
                    });
                },
                next => deleteBucket(bucket, next),
            ],
            done,
        );
    });

    it('should return QuotaExceeded when trying to copyObject in a versioned bucket with quota', done => {
        const bucket = 'quota-test-bucket12';
        const key = 'quota-test-object';
        const size = 900;
        let vID = null;
        return async.series(
            [
                next => createBucket(bucket, false, next),
                next => configureBucketVersioning(bucket, next),
                next =>
                    sendRequest(putQuotaVerb, '127.0.0.1:8000', `/${bucket}/?quota=true`, JSON.stringify(quota), config)
                        .then(() => next())
                        .catch(err => next(err)),
                next =>
                    putObject(bucket, key, size, (err, data) => {
                        assert.ifError(err);
                        vID = data.VersionId;
                        return next();
                    }),
                next => wait(inflightFlushFrequencyMS * 2, next),
                next =>
                    copyObject(bucket, key, size, err => {
                        try {
                            assert.strictEqual(err.name, 'QuotaExceeded');
                            return next();
                        } catch (assertError) {
                            return next(assertError);
                        }
                    }),
                next => deleteVersionID(bucket, key, vID, size, next),
                next => deleteBucket(bucket, next),
            ],
            done,
        );
    });

    it('should return QuotaExceeded when trying to CopyObject in a bucket with quota', done => {
        const bucket = 'quota-test-bucket2';
        const key = 'quota-test-object';
        const size = 900;
        return async.series(
            [
                next => createBucket(bucket, false, next),
                next =>
                    sendRequest(putQuotaVerb, '127.0.0.1:8000', `/${bucket}/?quota=true`, JSON.stringify(quota), config)
                        .then(() => next())
                        .catch(err => next(err)),
                next => putObject(bucket, key, size, next),
                next => wait(inflightFlushFrequencyMS * 2, next),
                next =>
                    copyObject(bucket, key, size, err => {
                        try {
                            assert.strictEqual(err.name, 'QuotaExceeded');
                            return next();
                        } catch (assertError) {
                            return next(assertError);
                        }
                    }),
                next => deleteObject(bucket, key, size, next),
                next => deleteBucket(bucket, next),
            ],
            done,
        );
    });

    it('should return QuotaExceeded when trying to complete MPU in a bucket with quota', done => {
        const bucket = 'quota-test-bucket3';
        const key = 'quota-test-object';
        const parts = 5;
        const partSize = 1024 * 1024 * 6;
        let uploadId = null;
        return async.series(
            [
                next => createBucket(bucket, false, next),
                next =>
                    sendRequest(putQuotaVerb, '127.0.0.1:8000', `/${bucket}/?quota=true`, JSON.stringify(quota), config)
                        .then(() => next())
                        .catch(err => next(err)),
                next =>
                    objectMPU(bucket, key, parts, partSize, (err, _uploadId) => {
                        uploadId = _uploadId;
                        try {
                            assert.strictEqual(err.name, 'QuotaExceeded');
                            return next();
                        } catch (assertError) {
                            return next(assertError);
                        }
                    }),
                next => abortMPU(bucket, key, uploadId, 0, next),
                next => wait(inflightFlushFrequencyMS * 2, next),
                next => {
                    assert.strictEqual(scuba.getInflightsForBucket(bucket), 0);
                    return next();
                },
                next => deleteBucket(bucket, next),
            ],
            done,
        );
    });

    it('should not return QuotaExceeded if the quota is not exceeded', done => {
        const bucket = 'quota-test-bucket4';
        const key = 'quota-test-object';
        const size = 300;
        return async.series(
            [
                next => createBucket(bucket, false, next),
                next =>
                    sendRequest(putQuotaVerb, '127.0.0.1:8000', `/${bucket}/?quota=true`, JSON.stringify(quota), config)
                        .then(() => next())
                        .catch(err => next(err)),
                next =>
                    putObject(bucket, key, size, err => {
                        assert.ifError(err);
                        return next();
                    }),
                next => deleteObject(bucket, key, size, next),
                next => deleteBucket(bucket, next),
            ],
            done,
        );
    });

    it('should not evaluate quotas if the backend is not available', done => {
        scuba.stop();
        const bucket = 'quota-test-bucket5';
        const key = 'quota-test-object';
        const size = 1024;
        return async.series(
            [
                next => createBucket(bucket, false, next),
                next =>
                    sendRequest(putQuotaVerb, '127.0.0.1:8000', `/${bucket}/?quota=true`, JSON.stringify(quota), config)
                        .then(() => next())
                        .catch(err => next(err)),
                next =>
                    putObject(bucket, key, size, err => {
                        assert.ifError(err);
                        return next();
                    }),
                next => deleteObject(bucket, key, size, next),
                next => deleteBucket(bucket, next),
            ],
            err => {
                assert.ifError(err);
                scuba.start();
                return wait(2000, done);
            },
        );
    });

    it('should return QuotaExceeded when trying to copy a part in a bucket with quota', done => {
        const bucket = 'quota-test-bucket6';
        const key = 'quota-test-object-copy';
        const keyToCopy = 'quota-test-existing';
        const parts = 5;
        const partSize = 1024 * 1024 * 6;
        let uploadId = null;
        return async.series(
            [
                next => createBucket(bucket, false, next),
                next =>
                    sendRequest(
                        putQuotaVerb,
                        '127.0.0.1:8000',
                        `/${bucket}/?quota=true`,
                        JSON.stringify({ quota: Math.round(partSize * 2.5) }),
                        config,
                    )
                        .then(() => next())
                        .catch(err => next(err)),
                next => putObject(bucket, keyToCopy, partSize, next),
                next =>
                    uploadPartCopy(
                        bucket,
                        key,
                        parts,
                        partSize,
                        inflightFlushFrequencyMS * 2,
                        keyToCopy,
                        (err, _uploadId) => {
                            uploadId = _uploadId;
                            try {
                                assert.strictEqual(err.name, 'QuotaExceeded');
                                return next();
                            } catch (assertError) {
                                return next(assertError);
                            }
                        },
                    ),
                next => abortMPU(bucket, key, uploadId, parts * partSize, next),
                next => deleteObject(bucket, keyToCopy, partSize, next),
                next => deleteBucket(bucket, next),
            ],
            done,
        );
    });

    it('should return QuotaExceeded when trying to restore an object in a bucket with quota', done => {
        const bucket = 'quota-test-bucket7';
        const key = 'quota-test-object';
        const size = 900;
        let vID = null;
        return async.series(
            [
                next => createBucket(bucket, false, next),
                next => configureBucketVersioning(bucket, next),
                next =>
                    sendRequest(putQuotaVerb, '127.0.0.1:8000', `/${bucket}/?quota=true`, JSON.stringify(quota), config)
                        .then(() => next())
                        .catch(err => next(err)),
                next =>
                    putObject(bucket, key, size, (err, data) => {
                        assert.ifError(err);
                        vID = data.VersionId;
                        return next();
                    }),
                next =>
                    fakeMetadataArchive(
                        bucket,
                        key,
                        vID,
                        {
                            archiveInfo: {},
                        },
                        next,
                    ),
                next => wait(inflightFlushFrequencyMS * 2, next),
                next =>
                    restoreObject(bucket, key, size, err => {
                        try {
                            assert.strictEqual(err.name, 'QuotaExceeded');
                            return next();
                        } catch (assertError) {
                            return next(assertError);
                        }
                    }),
                next => deleteVersionID(bucket, key, vID, size, next),
                next => deleteBucket(bucket, next),
            ],
            done,
        );
    });

    it('should not update the inflights if the quota check is passing but the object is already restored', done => {
        const bucket = 'quota-test-bucket14';
        const key = 'quota-test-object';
        const size = 100;
        let vID = null;
        return async.series(
            [
                next => createBucket(bucket, false, next),
                next => configureBucketVersioning(bucket, next),
                next =>
                    sendRequest(putQuotaVerb, '127.0.0.1:8000', `/${bucket}/?quota=true`, JSON.stringify(quota), config)
                        .then(() => next())
                        .catch(err => next(err)),
                next =>
                    putObject(bucket, key, size, (err, data) => {
                        assert.ifError(err);
                        vID = data.VersionId;
                        return next();
                    }),
                next =>
                    fakeMetadataArchive(
                        bucket,
                        key,
                        vID,
                        {
                            archiveInfo: {},
                            restoreRequestedAt: new Date(0).toString(),
                            restoreCompletedAt: new Date(0).toString() + 1,
                            restoreRequestedDays: 5,
                        },
                        next,
                    ),
                next => wait(inflightFlushFrequencyMS * 2, next),
                next => {
                    assert.strictEqual(scuba.getInflightsForBucket(bucket), size);
                    return next();
                },
                next => restoreObject(bucket, key, 0, next),
                next => wait(inflightFlushFrequencyMS * 2, next),
                next => {
                    assert.strictEqual(scuba.getInflightsForBucket(bucket), size);
                    return next();
                },
                next => deleteVersionID(bucket, key, vID, size, next),
                next => deleteBucket(bucket, next),
            ],
            done,
        );
    });

    it('should allow writes after deleting data with quotas', done => {
        const bucket = 'quota-test-bucket8';
        const key = 'quota-test-object';
        const size = 400;
        return async.series(
            [
                next => createBucket(bucket, false, next),
                next =>
                    sendRequest(putQuotaVerb, '127.0.0.1:8000', `/${bucket}/?quota=true`, JSON.stringify(quota), config)
                        .then(() => next())
                        .catch(err => next(err)),
                next =>
                    putObject(bucket, `${key}1`, size, err => {
                        assert.ifError(err);
                        return next();
                    }),
                next =>
                    putObject(bucket, `${key}2`, size, err => {
                        assert.ifError(err);
                        return next();
                    }),
                next => wait(inflightFlushFrequencyMS * 2, next),
                next =>
                    putObject(bucket, `${key}3`, size, err => {
                        try {
                            assert.strictEqual(err.name, 'QuotaExceeded');
                            return next();
                        } catch (assertError) {
                            return next(assertError);
                        }
                    }),
                next => wait(inflightFlushFrequencyMS * 2, next),
                next => {
                    assert.strictEqual(scuba.getInflightsForBucket(bucket), size * 2);
                    return next();
                },
                next => wait(inflightFlushFrequencyMS * 2, next),
                next => deleteObject(bucket, `${key}2`, size, next),
                next => wait(inflightFlushFrequencyMS * 2, next),
                next =>
                    putObject(bucket, `${key}4`, size, err => {
                        assert.ifError(err);
                        return next();
                    }),
                next => deleteObject(bucket, `${key}1`, size, next),
                next => deleteObject(bucket, `${key}3`, size, next),
                next => deleteObject(bucket, `${key}4`, size, next),
                next => deleteBucket(bucket, next),
            ],
            done,
        );
    });

    it('should allow writes after deleting data with quotas below the current number of inflights', done => {
        const bucket = 'quota-test-bucket8';
        const key = 'quota-test-object';
        const size = 400;
        if (!s3Config.isQuotaInflightEnabled()) {
            return done();
        }
        return async.series(
            [
                next => createBucket(bucket, false, next),
                // Set the quota to 10 * size (4000)
                next =>
                    sendRequest(
                        putQuotaVerb,
                        '127.0.0.1:8000',
                        `/${bucket}/?quota=true`,
                        JSON.stringify({ quota: 10 * size }),
                        config,
                    )
                        .then(() => next())
                        .catch(err => next(err)),
                // Simulate previous operations since last metrics update (4000 bytes)
                next =>
                    putObject(bucket, `${key}1`, 5 * size, err => {
                        assert.ifError(err);
                        return next();
                    }),
                next =>
                    putObject(bucket, `${key}2`, 5 * size, err => {
                        assert.ifError(err);
                        return next();
                    }),
                next => wait(inflightFlushFrequencyMS * 2, next),
                // After metrics update, set the inflights to 0 (simulate end of metrics update)
                next => {
                    scuba.setInflightAsCapacity(bucket);
                    return next();
                },
                // Here we have 0 inflight but the stored bytes are 4000 (equal to the quota)
                // Should reject new write with QuotaExceeded (4000 + 400)
                next =>
                    putObject(bucket, `${key}3`, size, err => {
                        try {
                            assert.strictEqual(err.name, 'QuotaExceeded');
                            return next();
                        } catch (assertError) {
                            return next(assertError);
                        }
                    }),
                next => wait(inflightFlushFrequencyMS * 2, next),
                // Should still have 0 as inflight
                next => {
                    assert.strictEqual(scuba.getInflightsForBucket(bucket), 0);
                    return next();
                },
                next => wait(inflightFlushFrequencyMS * 2, next),
                // Now delete one object (2000 bytes), it should let us write again
                next => deleteObject(bucket, `${key}1`, size, next),
                next => wait(inflightFlushFrequencyMS * 2, next),
                next =>
                    putObject(bucket, `${key}4`, 5 * size, err => {
                        assert.ifError(err);
                        return next();
                    }),
                // Cleanup
                next => deleteObject(bucket, `${key}2`, size, next),
                next => deleteObject(bucket, `${key}4`, size, next),
                next => deleteBucket(bucket, next),
            ],
            done,
        );
    });

    it('should not increase the inflights when the object is being rewritten with a smaller object', done => {
        const bucket = 'quota-test-bucket9';
        const key = 'quota-test-object';
        const size = 400;
        return async.series(
            [
                next => createBucket(bucket, false, next),
                next =>
                    sendRequest(putQuotaVerb, '127.0.0.1:8000', `/${bucket}/?quota=true`, JSON.stringify(quota), config)
                        .then(() => next())
                        .catch(err => next(err)),
                next =>
                    putObject(bucket, key, size, err => {
                        assert.ifError(err);
                        return next();
                    }),
                next => wait(inflightFlushFrequencyMS * 2, next),
                next =>
                    putObject(bucket, key, size - 100, err => {
                        assert.ifError(err);
                        if (!s3Config.isQuotaInflightEnabled()) {
                            mockScuba.incrementBytesForBucket(bucket, -size);
                        }
                        return next();
                    }),
                next => wait(inflightFlushFrequencyMS * 2, next),
                next => {
                    assert.strictEqual(scuba.getInflightsForBucket(bucket), size - 100);
                    return next();
                },
                next => deleteObject(bucket, key, size, next),
                next => deleteBucket(bucket, next),
            ],
            done,
        );
    });
    it('should decrease the inflights when performing multi object delete', done => {
        const bucket = 'quota-test-bucket10';
        const key = 'quota-test-object';
        const size = 400;
        return async.series(
            [
                next => createBucket(bucket, false, next),
                next =>
                    sendRequest(putQuotaVerb, '127.0.0.1:8000', `/${bucket}/?quota=true`, JSON.stringify(quota), config)
                        .then(() => next())
                        .catch(err => next(err)),
                next => {
                    putObject(bucket, `${key}1`, size, err => {
                        assert.ifError(err);
                        return next();
                    });
                },
                next => {
                    putObject(bucket, `${key}2`, size, err => {
                        assert.ifError(err);
                        return next();
                    });
                },
                next => wait(inflightFlushFrequencyMS * 2, next),
                next =>
                    multiObjectDelete(bucket, [`${key}1`, `${key}2`], size * 2, err => {
                        assert.ifError(err);
                        return next();
                    }),
                next => wait(inflightFlushFrequencyMS * 2, next),
                next => {
                    assert.strictEqual(scuba.getInflightsForBucket(bucket), 0);
                    return next();
                },
                next => {
                    deleteBucket(bucket, next);
                },
            ],
            done,
        );
    });

    it('should allow writes after multi-deleting data with quotas below the current number of inflights', done => {
        const bucket = 'quota-test-bucket10';
        const key = 'quota-test-object';
        const size = 400;
        if (!s3Config.isQuotaInflightEnabled()) {
            return done();
        }
        return async.series(
            [
                next => createBucket(bucket, false, next),
                next =>
                    sendRequest(
                        putQuotaVerb,
                        '127.0.0.1:8000',
                        `/${bucket}/?quota=true`,
                        JSON.stringify({ quota: size * 10 }),
                        config,
                    )
                        .then(() => next())
                        .catch(err => next(err)),
                next =>
                    putObject(bucket, `${key}1`, size * 5, err => {
                        assert.ifError(err);
                        return next();
                    }),
                next =>
                    putObject(bucket, `${key}2`, size * 5, err => {
                        assert.ifError(err);
                        return next();
                    }),
                next => wait(inflightFlushFrequencyMS * 2, next),
                next => {
                    scuba.setInflightAsCapacity(bucket);
                    return next();
                },
                next =>
                    putObject(bucket, `${key}3`, size, err => {
                        try {
                            assert.strictEqual(err.name, 'QuotaExceeded');
                            return next();
                        } catch (assertError) {
                            return next(assertError);
                        }
                    }),
                next => wait(inflightFlushFrequencyMS * 2, next),
                next => {
                    assert.strictEqual(scuba.getInflightsForBucket(bucket), 0);
                    return next();
                },
                next =>
                    multiObjectDelete(bucket, [`${key}1`, `${key}2`], size * 10, err => {
                        assert.ifError(err);
                        return next();
                    }),
                next => wait(inflightFlushFrequencyMS * 2, next),
                next =>
                    putObject(bucket, `${key}4`, size * 5, err => {
                        assert.ifError(err);
                        return next();
                    }),
                next => deleteObject(bucket, `${key}4`, size * 5, next),
                next => deleteBucket(bucket, next),
            ],
            done,
        );
    });

    it('should not update the inflights if the API errored after evaluating quotas (deletion)', done => {
        const bucket = 'quota-test-bucket11';
        const key = 'quota-test-object';
        const size = 100;
        let vID = null;
        return async.series(
            [
                next => createBucket(bucket, true, next),
                next => putObjectLockConfiguration(bucket, next),
                next =>
                    sendRequest(putQuotaVerb, '127.0.0.1:8000', `/${bucket}/?quota=true`, JSON.stringify(quota), config)
                        .then(() => next())
                        .catch(err => next(err)),
                next =>
                    putObject(bucket, key, size, (err, val) => {
                        assert.ifError(err);
                        vID = val.VersionId;
                        return next();
                    }),
                next => wait(inflightFlushFrequencyMS * 2, next),
                next => {
                    assert.strictEqual(scuba.getInflightsForBucket(bucket), size);
                    return next();
                },
                next =>
                    deleteVersionID(bucket, key, vID, size, err => {
                        try {
                            assert.strictEqual(err.name, 'AccessDenied');
                            next();
                        } catch (assertError) {
                            next(assertError);
                        }
                    }),
                next => wait(inflightFlushFrequencyMS * 2, next),
                next => {
                    assert.strictEqual(scuba.getInflightsForBucket(bucket), size);
                    return next();
                },
            ],
            done,
        );
    });

    it('should only evaluate quota and not update inflights for PutObject with the x-scal-s3-version-id header', done => {
        const bucket = 'quota-test-bucket13';
        const key = 'quota-test-object';
        const size = 100;
        let vID = null;
        return async.series(
            [
                next => createBucket(bucket, true, next),
                next =>
                    sendRequest(putQuotaVerb, '127.0.0.1:8000', `/${bucket}/?quota=true`, JSON.stringify(quota), config)
                        .then(() => next())
                        .catch(err => next(err)),
                next =>
                    putObject(bucket, key, size, (err, val) => {
                        assert.ifError(err);
                        vID = val.VersionId;
                        return next();
                    }),
                next => wait(inflightFlushFrequencyMS * 2, next),
                next => {
                    assert.strictEqual(scuba.getInflightsForBucket(bucket), size);
                    return next();
                },
                next =>
                    fakeMetadataArchive(
                        bucket,
                        key,
                        vID,
                        {
                            archiveInfo: {},
                            restoreRequestedAt: new Date(0).toISOString(),
                            restoreRequestedDays: 7,
                        },
                        next,
                    ),
                // Simulate the real restore
                next =>
                    putObjectWithCustomHeader(bucket, key, size, vID, err => {
                        assert.ifError(err);
                        return next();
                    }),
                next => {
                    assert.strictEqual(scuba.getInflightsForBucket(bucket), size);
                    return next();
                },
                next => deleteVersionID(bucket, key, vID, size, next),
                next => deleteBucket(bucket, next),
            ],
            done,
        );
    });

    it('should allow a restore if the quota is full but the objet fits with its reserved storage space', done => {
        const bucket = 'quota-test-bucket15';
        const key = 'quota-test-object';
        const size = 1000;
        let vID = null;
        return async.series(
            [
                next => createBucket(bucket, true, next),
                next =>
                    sendRequest(putQuotaVerb, '127.0.0.1:8000', `/${bucket}/?quota=true`, JSON.stringify(quota), config)
                        .then(() => next())
                        .catch(err => next(err)),
                next =>
                    putObject(bucket, key, size, (err, val) => {
                        assert.ifError(err);
                        vID = val.VersionId;
                        return next();
                    }),
                next => wait(inflightFlushFrequencyMS * 2, next),
                next => {
                    assert.strictEqual(scuba.getInflightsForBucket(bucket), size);
                    return next();
                },
                next =>
                    fakeMetadataArchive(
                        bucket,
                        key,
                        vID,
                        {
                            archiveInfo: {},
                            restoreRequestedAt: new Date(0).toISOString(),
                            restoreRequestedDays: 7,
                        },
                        next,
                    ),
                // Put an object, the quota should be exceeded
                next =>
                    putObject(bucket, `${key}-2`, size, err => {
                        try {
                            assert.strictEqual(err.name, 'QuotaExceeded');
                            return next();
                        } catch (assertError) {
                            return next(assertError);
                        }
                    }),
                next => {
                    assert.strictEqual(scuba.getInflightsForBucket(bucket), size);
                    return next();
                },
                next => deleteVersionID(bucket, key, vID, size, next),
                next => deleteBucket(bucket, next),
            ],
            done,
        );
    });

    it('should reduce inflights when completing MPU with fewer parts than uploaded', done => {
        const bucket = 'quota-test-bucket-mpu1';
        const key = 'quota-test-object';
        const parts = 3;
        const partSize = 5 * 1024 * 1024;
        const totalSize = parts * partSize;
        const usedParts = 2;
        let uploadId = null;
        const ETags = [];

        if (!s3Config.isQuotaInflightEnabled()) {
            return done();
        }

        return async.series(
            [
                next => createBucket(bucket, false, next),
                next =>
                    sendRequest(
                        putQuotaVerb,
                        '127.0.0.1:8000',
                        `/${bucket}/?quota=true`,
                        JSON.stringify({ quota: totalSize * 2 }),
                        config,
                    )
                        .then(() => next())
                        .catch(err => next(err)),
                next =>
                    s3Client
                        .send(
                            new CreateMultipartUploadCommand({
                                Bucket: bucket,
                                Key: key,
                            }),
                        )
                        .then(data => {
                            uploadId = data.UploadId;
                            return next();
                        })
                        .catch(err => next(err)),
                next =>
                    async.timesSeries(
                        parts,
                        (n, cb) => {
                            const uploadPartParams = {
                                Bucket: bucket,
                                Key: key,
                                PartNumber: n + 1,
                                UploadId: uploadId,
                                Body: Buffer.alloc(partSize),
                            };
                            return s3Client
                                .send(new UploadPartCommand(uploadPartParams))
                                .then(data => {
                                    ETags[n] = data.ETag;
                                    return cb();
                                })
                                .catch(cb);
                        },
                        next,
                    ),
                next => wait(inflightFlushFrequencyMS * 2, next),
                next => {
                    // Verify all parts are counted in inflights
                    assert.strictEqual(scuba.getInflightsForBucket(bucket), totalSize);
                    return next();
                },
                next => {
                    // Complete with only first two parts
                    const params = {
                        Bucket: bucket,
                        Key: key,
                        MultipartUpload: {
                            Parts: Array.from({ length: usedParts }, (_, i) => ({
                                ETag: ETags[i],
                                PartNumber: i + 1,
                            })),
                        },
                        UploadId: uploadId,
                    };
                    return s3Client
                        .send(new CompleteMultipartUploadCommand(params))
                        .then(() => next())
                        .catch(err => next(err));
                },
                next => wait(inflightFlushFrequencyMS * 2, () => next()),
                next => {
                    // Verify inflights reduced by dropped part
                    const expectedInflights = usedParts * partSize;
                    assert.strictEqual(scuba.getInflightsForBucket(bucket), expectedInflights);
                    return next();
                },
                next => deleteObject(bucket, key, usedParts * partSize, next),
                next => deleteBucket(bucket, next),
            ],
            done,
        );
    });

    it('should reduce inflights when aborting MPU', done => {
        const bucket = 'quota-test-bucket-mpu2';
        const key = 'quota-test-object';
        const parts = 3;
        const partSize = 5 * 1024 * 1024;
        const totalSize = parts * partSize;
        let uploadId = null;

        if (!s3Config.isQuotaInflightEnabled()) {
            return done();
        }

        return async.series(
            [
                next => createBucket(bucket, false, next),
                next =>
                    sendRequest(
                        putQuotaVerb,
                        '127.0.0.1:8000',
                        `/${bucket}/?quota=true`,
                        JSON.stringify({ quota: totalSize * 2 }),
                        config,
                    )
                        .then(() => next())
                        .catch(err => next(err)),
                next =>
                    s3Client
                        .send(
                            new CreateMultipartUploadCommand({
                                Bucket: bucket,
                                Key: key,
                            }),
                        )
                        .then(data => {
                            uploadId = data.UploadId;
                            return next();
                        })
                        .catch(err => next(err)),
                next =>
                    async.timesSeries(
                        parts,
                        (n, cb) => {
                            const uploadPartParams = {
                                Bucket: bucket,
                                Key: key,
                                PartNumber: n + 1,
                                UploadId: uploadId,
                                Body: Buffer.alloc(partSize),
                            };
                            return s3Client
                                .send(new UploadPartCommand(uploadPartParams))
                                .then(data => cb(null, data))
                                .catch(cb);
                        },
                        next,
                    ),
                next => wait(inflightFlushFrequencyMS * 2, next),
                next => {
                    // Verify all parts are counted in inflights
                    assert.strictEqual(scuba.getInflightsForBucket(bucket), totalSize);
                    return next();
                },
                next => abortMPU(bucket, key, uploadId, totalSize, next),
                next => wait(inflightFlushFrequencyMS * 2, next),
                next => {
                    // Verify inflights reduced to zero after abort
                    assert.strictEqual(scuba.getInflightsForBucket(bucket), 0);
                    return next();
                },
                next => deleteBucket(bucket, next),
            ],
            done,
        );
    });
});
