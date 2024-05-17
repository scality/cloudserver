const async = require('async');
const assert = require('assert');
const { S3 } = require('aws-sdk');
const getConfig = require('../functional/aws-node-sdk/test/support/config');
const { Scuba: MockScuba, inflightFlushFrequencyMS } = require('../utilities/mock/Scuba');
const sendRequest = require('../functional/aws-node-sdk/test/quota/tooling').sendRequest;
const memCredentials = require('../functional/aws-node-sdk/lib/json/mem_credentials.json');
const metadata = require('../../lib/metadata/wrapper');
const { fakeMetadataArchive } = require('../functional/aws-node-sdk/test/utils/init');
const { config: s3Config } = require('../../lib/Config');

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
    return s3Client.createBucket(config, (err, data) => {
        assert.ifError(err);
        return cb(err, data);
    });
}

function configureBucketVersioning(bucket, cb) {
    return s3Client.putBucketVersioning({
        Bucket: bucket,
        VersioningConfiguration: {
            Status: 'Enabled',
        },
    }, (err, data) => {
        assert.ifError(err);
        return cb(err, data);
    });
}

function putObjectLockConfiguration(bucket, cb) {
    return s3Client.putObjectLockConfiguration({
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
    }, (err, data) => {
        assert.ifError(err);
        return cb(err, data);
    });
}

function deleteBucket(bucket, cb) {
    return s3Client.deleteBucket({
        Bucket: bucket,
    }, err => {
        assert.ifError(err);
        return cb(err);
    });
}

function putObject(bucket, key, size, cb) {
    return s3Client.putObject({
        Bucket: bucket,
        Key: key,
        Body: Buffer.alloc(size),
    }, (err, data) => {
        if (!err && !s3Config.isQuotaInflightEnabled()) {
            mockScuba.incrementBytesForBucket(bucket, size);
        }
        return cb(err, data);
    });
}

function putObjectWithCustomHeader(bucket, key, size, vID, cb) {
    const request = s3Client.putObject({
        Bucket: bucket,
        Key: key,
        Body: Buffer.alloc(size),
    });

    request.on('build', () => {
        request.httpRequest.headers['x-scal-s3-version-id'] = vID;
    });

    return request.send((err, data) => {
        if (!err && !s3Config.isQuotaInflightEnabled()) {
            mockScuba.incrementBytesForBucket(bucket, 0);
        }
        return cb(err, data);
    });
}

function copyObject(bucket, key, sourceSize, cb) {
    return s3Client.copyObject({
        Bucket: bucket,
        CopySource: `/${bucket}/${key}`,
        Key: `${key}-copy`,
    }, (err, data) => {
        if (!err && !s3Config.isQuotaInflightEnabled()) {
            mockScuba.incrementBytesForBucket(bucket, sourceSize);
        }
        return cb(err, data);
    });
}

function deleteObject(bucket, key, size, cb) {
    return s3Client.deleteObject({
        Bucket: bucket,
        Key: key,
    }, err => {
        if (!err && !s3Config.isQuotaInflightEnabled()) {
            mockScuba.incrementBytesForBucket(bucket, -size);
        }
        assert.ifError(err);
        return cb(err);
    });
}

function deleteVersionID(bucket, key, versionId, size, cb) {
    return s3Client.deleteObject({
        Bucket: bucket,
        Key: key,
        VersionId: versionId,
    }, (err, data) => {
        if (!err && !s3Config.isQuotaInflightEnabled()) {
            mockScuba.incrementBytesForBucket(bucket, -size);
        }
        return cb(err, data);
    });
}

function objectMPU(bucket, key, parts, partSize, callback) {
    let ETags = [];
    let uploadId = null;
    const partNumbers = Array.from(Array(parts).keys());
    const initiateMPUParams = {
        Bucket: bucket,
        Key: key,
    };
    return async.waterfall([
        next => s3Client.createMultipartUpload(initiateMPUParams,
            (err, data) => {
                if (err) {
                    return next(err);
                }
                uploadId = data.UploadId;
                return next();
            }),
        next =>
            async.mapLimit(partNumbers, 1, (partNumber, callback) => {
                const uploadPartParams = {
                    Bucket: bucket,
                    Key: key,
                    PartNumber: partNumber + 1,
                    UploadId: uploadId,
                    Body: Buffer.alloc(partSize),
                };

                return s3Client.uploadPart(uploadPartParams,
                    (err, data) => {
                        if (err) {
                            return callback(err);
                        }
                        return callback(null, data.ETag);
                    });
            }, (err, results) => {
                if (err) {
                    return next(err);
                }
                ETags = results;
                return next();
            }),
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
            return s3Client.completeMultipartUpload(params, next);
        },
    ], err => {
        if (!err && !s3Config.isQuotaInflightEnabled()) {
            mockScuba.incrementBytesForBucket(bucket, parts * partSize);
        }
        return callback(err, uploadId);
    });
}

function abortMPU(bucket, key, uploadId, size, callback) {
    return s3Client.abortMultipartUpload({
        Bucket: bucket,
        Key: key,
        UploadId: uploadId,
    }, (err, data) => {
        if (!err && !s3Config.isQuotaInflightEnabled()) {
            mockScuba.incrementBytesForBucket(bucket, -size);
        }
        return callback(err, data);
    });
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
    return async.waterfall([
        next => s3Client.createMultipartUpload(initiateMPUParams,
            (err, data) => {
                if (err) {
                    return next(err);
                }
                uploadId = data.UploadId;
                return next();
            }),
        next => {
            const uploadPartParams = {
                Bucket: bucket,
                Key: key,
                PartNumber: partNumber + 1,
                UploadId: uploadId,
                Body: Buffer.alloc(partSize),
            };
            return s3Client.uploadPart(uploadPartParams, (err, data) => {
                if (err) {
                    return next(err);
                }
                ETags[partNumber] = data.ETag;
                return next();
            });
        },
        next => wait(sleepDuration, next),
        next => {
            const copyPartParams = {
                Bucket: bucket,
                CopySource: `/${bucket}/${keyToCopy}`,
                Key: `${key}-copy`,
                PartNumber: partNumber + 1,
                UploadId: uploadId,
            };
            return s3Client.uploadPartCopy(copyPartParams, (err, data) => {
                if (err) {
                    return next(err);
                }
                ETags[partNumber] = data.ETag;
                return next(null, data.ETag);
            });
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
            return s3Client.completeMultipartUpload(params, next);
        },
    ], err => {
        if (err && !s3Config.isQuotaInflightEnabled()) {
            mockScuba.incrementBytesForBucket(bucket, -(parts * partSize));
        }
        return callback(err, uploadId);
    });
}

function restoreObject(bucket, key, size, callback) {
    return s3Client.restoreObject({
        Bucket: bucket,
        Key: key,
        RestoreRequest: {
            Days: 1,
        },
    }, (err, data) => {
        if (!err && !s3Config.isQuotaInflightEnabled()) {
            mockScuba.incrementBytesForBucket(bucket, size);
        }
        return callback(err, data);
    });
}

function multiObjectDelete(bucket, keys, size, callback) {
    if (!s3Config.isQuotaInflightEnabled()) {
        mockScuba.incrementBytesForBucket(bucket, -size);
    }
    return s3Client.deleteObjects({
        Bucket: bucket,
        Delete: {
            Objects: keys.map(key => ({ Key: key })),
        },
    }, (err, data) => {
        if (err && !s3Config.isQuotaInflightEnabled()) {
            mockScuba.incrementBytesForBucket(bucket, size);
        }
        return callback(err, data);
    });
}

(process.env.S3METADATA === 'mongodb' ? describe : describe.skip)('quota evaluation with scuba metrics',
    function t() {
        this.timeout(30000);
        const scuba = new MockScuba();
        const putQuotaVerb = 'PUT';
        const config = {
            accessKey: memCredentials.default.accessKey,
            secretKey: memCredentials.default.secretKey,
        };
        mockScuba = scuba;

        before(done => {
            const config = getConfig('default', { signatureVersion: 'v4', maxRetries: 0 });
            s3Client = new S3(config);
            scuba.start();
            return metadata.setup(err => wait(2000, () => done(err)));
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
            return async.series([
                next => createBucket(bucket, false, next),
                next => sendRequest(putQuotaVerb, '127.0.0.1:8000', `/${bucket}/?quota=true`,
                    JSON.stringify(quota), config).then(() => next()).catch(err => next(err)),
                next => putObject(bucket, key, size, err => {
                    assert.strictEqual(err.code, 'QuotaExceeded');
                    return next();
                }),
                next => deleteBucket(bucket, next),
            ], done);
        });

        it('should return QuotaExceeded when trying to copyObject in a versioned bucket with quota', done => {
            const bucket = 'quota-test-bucket12';
            const key = 'quota-test-object';
            const size = 900;
            let vID = null;
            return async.series([
                next => createBucket(bucket, false, next),
                next => configureBucketVersioning(bucket, next),
                next => sendRequest(putQuotaVerb, '127.0.0.1:8000', `/${bucket}/?quota=true`,
                    JSON.stringify(quota), config).then(() => next()).catch(err => next(err)),
                next => putObject(bucket, key, size, (err, data) => {
                    assert.ifError(err);
                    vID = data.VersionId;
                    return next();
                }),
                next => wait(inflightFlushFrequencyMS * 2, next),
                next => copyObject(bucket, key, size, err => {
                    assert.strictEqual(err.code, 'QuotaExceeded');
                    return next();
                }),
                next => deleteVersionID(bucket, key, vID, size, next),
                next => deleteBucket(bucket, next),
            ], done);
        });

        it('should return QuotaExceeded when trying to CopyObject in a bucket with quota', done => {
            const bucket = 'quota-test-bucket2';
            const key = 'quota-test-object';
            const size = 900;
            return async.series([
                next => createBucket(bucket, false, next),
                next => sendRequest(putQuotaVerb, '127.0.0.1:8000', `/${bucket}/?quota=true`,
                    JSON.stringify(quota), config).then(() => next()).catch(err => next(err)),
                next => putObject(bucket, key, size, next),
                next => wait(inflightFlushFrequencyMS * 2, next),
                next => copyObject(bucket, key, size, err => {
                    assert.strictEqual(err.code, 'QuotaExceeded');
                    return next();
                }),
                next => deleteObject(bucket, key, size, next),
                next => deleteBucket(bucket, next),
            ], done);
        });

        it('should return QuotaExceeded when trying to complete MPU in a bucket with quota', done => {
            const bucket = 'quota-test-bucket3';
            const key = 'quota-test-object';
            const parts = 5;
            const partSize = 1024 * 1024 * 6;
            let uploadId = null;
            return async.series([
                next => createBucket(bucket, false, next),
                next => sendRequest(putQuotaVerb, '127.0.0.1:8000', `/${bucket}/?quota=true`,
                    JSON.stringify(quota), config).then(() => next()).catch(err => next(err)),
                next => objectMPU(bucket, key, parts, partSize, (err, _uploadId) => {
                    uploadId = _uploadId;
                    assert.strictEqual(err.code, 'QuotaExceeded');
                    return next();
                }),
                next => abortMPU(bucket, key, uploadId, 0, next),
                next => wait(inflightFlushFrequencyMS * 2, next),
                next => {
                    assert.strictEqual(scuba.getInflightsForBucket(bucket), 0);
                    return next();
                },
                next => deleteBucket(bucket, next),
            ], done);
        });

        it('should not return QuotaExceeded if the quota is not exceeded', done => {
            const bucket = 'quota-test-bucket4';
            const key = 'quota-test-object';
            const size = 300;
            return async.series([
                next => createBucket(bucket, false, next),
                next => sendRequest(putQuotaVerb, '127.0.0.1:8000', `/${bucket}/?quota=true`,
                    JSON.stringify(quota), config).then(() => next()).catch(err => next(err)),
                next => putObject(bucket, key, size, err => {
                    assert.ifError(err);
                    return next();
                }),
                next => deleteObject(bucket, key, size, next),
                next => deleteBucket(bucket, next),
            ], done);
        });

        it('should not evaluate quotas if the backend is not available', done => {
            scuba.stop();
            const bucket = 'quota-test-bucket5';
            const key = 'quota-test-object';
            const size = 1024;
            return async.series([
                next => createBucket(bucket, false, next),
                next => sendRequest(putQuotaVerb, '127.0.0.1:8000', `/${bucket}/?quota=true`,
                    JSON.stringify(quota), config).then(() => next()).catch(err => next(err)),
                next => putObject(bucket, key, size, err => {
                    assert.ifError(err);
                    return next();
                }),
                next => deleteObject(bucket, key, size, next),
                next => deleteBucket(bucket, next),
            ], err => {
                assert.ifError(err);
                scuba.start();
                return wait(2000, done);
            });
        });

        it('should return QuotaExceeded when trying to copy a part in a bucket with quota', done => {
            const bucket = 'quota-test-bucket6';
            const key = 'quota-test-object-copy';
            const keyToCopy = 'quota-test-existing';
            const parts = 5;
            const partSize = 1024 * 1024 * 6;
            let uploadId = null;
            return async.series([
                next => createBucket(bucket, false, next),
                next => sendRequest(putQuotaVerb, '127.0.0.1:8000', `/${bucket}/?quota=true`,
                    JSON.stringify({ quota: Math.round(partSize * 2.5) }), config)
                    .then(() => next()).catch(err => next(err)),
                next => putObject(bucket, keyToCopy, partSize, next),
                next => uploadPartCopy(bucket, key, parts, partSize, inflightFlushFrequencyMS * 2, keyToCopy,
                    (err, _uploadId) => {
                        uploadId = _uploadId;
                        assert.strictEqual(err.code, 'QuotaExceeded');
                        return next();
                    }),
                next => abortMPU(bucket, key, uploadId, parts * partSize, next),
                next => deleteObject(bucket, keyToCopy, partSize, next),
                next => deleteBucket(bucket, next),
            ], done);
        });

        it('should return QuotaExceeded when trying to restore an object in a bucket with quota', done => {
            const bucket = 'quota-test-bucket7';
            const key = 'quota-test-object';
            const size = 900;
            let vID = null;
            return async.series([
                next => createBucket(bucket, false, next),
                next => configureBucketVersioning(bucket, next),
                next => sendRequest(putQuotaVerb, '127.0.0.1:8000', `/${bucket}/?quota=true`,
                    JSON.stringify(quota), config).then(() => next()).catch(err => next(err)),
                next => putObject(bucket, key, size, (err, data) => {
                    assert.ifError(err);
                    vID = data.VersionId;
                    return next();
                }),
                next => fakeMetadataArchive(bucket, key, vID, {
                    archiveInfo: {},
                }, next),
                next => wait(inflightFlushFrequencyMS * 2, next),
                next => restoreObject(bucket, key, size, err => {
                    assert.strictEqual(err.code, 'QuotaExceeded');
                    return next();
                }),
                next => deleteVersionID(bucket, key, vID, size, next),
                next => deleteBucket(bucket, next),
            ], done);
        });

        it('should not update the inflights if the quota check is passing but the object is already restored', done => {
            const bucket = 'quota-test-bucket14';
            const key = 'quota-test-object';
            const size = 100;
            let vID = null;
            return async.series([
                next => createBucket(bucket, false, next),
                next => configureBucketVersioning(bucket, next),
                next => sendRequest(putQuotaVerb, '127.0.0.1:8000', `/${bucket}/?quota=true`,
                    JSON.stringify(quota), config).then(() => next()).catch(err => next(err)),
                next => putObject(bucket, key, size, (err, data) => {
                    assert.ifError(err);
                    vID = data.VersionId;
                    return next();
                }),
                next => fakeMetadataArchive(bucket, key, vID, {
                    archiveInfo: {},
                    restoreRequestedAt: new Date(0).toString(),
                    restoreCompletedAt: new Date(0).toString() + 1,
                    restoreRequestedDays: 5,
                }, next),
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
            ], done);
        });

        it('should allow writes after deleting data with quotas', done => {
            const bucket = 'quota-test-bucket8';
            const key = 'quota-test-object';
            const size = 400;
            return async.series([
                next => createBucket(bucket, false, next),
                next => sendRequest(putQuotaVerb, '127.0.0.1:8000', `/${bucket}/?quota=true`,
                    JSON.stringify(quota), config).then(() => next()).catch(err => next(err)),
                next => putObject(bucket, `${key}1`, size, err => {
                    assert.ifError(err);
                    return next();
                }),
                next => putObject(bucket, `${key}2`, size, err => {
                    assert.ifError(err);
                    return next();
                }),
                next => wait(inflightFlushFrequencyMS * 2, next),
                next => putObject(bucket, `${key}3`, size, err => {
                    assert.strictEqual(err.code, 'QuotaExceeded');
                    return next();
                }),
                next => wait(inflightFlushFrequencyMS * 2, next),
                next => {
                    assert.strictEqual(scuba.getInflightsForBucket(bucket), size * 2);
                    return next();
                },
                next => wait(inflightFlushFrequencyMS * 2, next),
                next => deleteObject(bucket, `${key}2`, size, next),
                next => wait(inflightFlushFrequencyMS * 2, next),
                next => putObject(bucket, `${key}4`, size, err => {
                    assert.ifError(err);
                    return next();
                }),
                next => deleteObject(bucket, `${key}1`, size, next),
                next => deleteObject(bucket, `${key}3`, size, next),
                next => deleteObject(bucket, `${key}4`, size, next),
                next => deleteBucket(bucket, next),
            ], done);
        });

        it('should not increase the inflights when the object is being rewritten with a smaller object', done => {
            const bucket = 'quota-test-bucket9';
            const key = 'quota-test-object';
            const size = 400;
            return async.series([
                next => createBucket(bucket, false, next),
                next => sendRequest(putQuotaVerb, '127.0.0.1:8000', `/${bucket}/?quota=true`,
                    JSON.stringify(quota), config).then(() => next()).catch(err => next(err)),
                next => putObject(bucket, key, size, err => {
                    assert.ifError(err);
                    return next();
                }),
                next => wait(inflightFlushFrequencyMS * 2, next),
                next => putObject(bucket, key, size - 100, err => {
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
            ], done);
        });

        it('should decrease the inflights when performing multi object delete', done => {
            const bucket = 'quota-test-bucket10';
            const key = 'quota-test-object';
            const size = 400;
            return async.series([
                next => createBucket(bucket, false, next),
                next => sendRequest(putQuotaVerb, '127.0.0.1:8000', `/${bucket}/?quota=true`,
                    JSON.stringify(quota), config).then(() => next()).catch(err => next(err)),
                next => putObject(bucket, `${key}1`, size, err => {
                    assert.ifError(err);
                    return next();
                }
                ),
                next => putObject(bucket, `${key}2`, size, err => {
                    assert.ifError(err);
                    return next();
                }),
                next => wait(inflightFlushFrequencyMS * 2, next),
                next => multiObjectDelete(bucket, [`${key}1`, `${key}2`], size * 2, err => {
                    assert.ifError(err);
                    return next();
                }),
                next => wait(inflightFlushFrequencyMS * 2, next),
                next => {
                    assert.strictEqual(scuba.getInflightsForBucket(bucket), 0);
                    return next();
                },
                next => deleteBucket(bucket, next),
            ], done);
        });

        it('should not update the inflights if the API errored after evaluating quotas (deletion)', done => {
            const bucket = 'quota-test-bucket11';
            const key = 'quota-test-object';
            const size = 100;
            let vID = null;
            return async.series([
                next => createBucket(bucket, true, next),
                next => putObjectLockConfiguration(bucket, next),
                next => sendRequest(putQuotaVerb, '127.0.0.1:8000', `/${bucket}/?quota=true`,
                    JSON.stringify(quota), config).then(() => next()).catch(err => next(err)),
                next => putObject(bucket, key, size, (err, val) => {
                    assert.ifError(err);
                    vID = val.VersionId;
                    return next();
                }),
                next => wait(inflightFlushFrequencyMS * 2, next),
                next => {
                    assert.strictEqual(scuba.getInflightsForBucket(bucket), size);
                    return next();
                },
                next => deleteVersionID(bucket, key, vID, size, err => {
                    assert.strictEqual(err.code, 'AccessDenied');
                    next();
                }),
                next => wait(inflightFlushFrequencyMS * 2, next),
                next => {
                    assert.strictEqual(scuba.getInflightsForBucket(bucket), size);
                    return next();
                },
            ], done);
        });

        it('should only evaluate quota and not update inflights for PutObject with the x-scal-s3-version-id header',
            done => {
                const bucket = 'quota-test-bucket13';
                const key = 'quota-test-object';
                const size = 100;
                let vID = null;
                return async.series([
                    next => createBucket(bucket, true, next),
                    next => sendRequest(putQuotaVerb, '127.0.0.1:8000', `/${bucket}/?quota=true`,
                        JSON.stringify(quota), config).then(() => next()).catch(err => next(err)),
                    next => putObject(bucket, key, size, (err, val) => {
                        assert.ifError(err);
                        vID = val.VersionId;
                        return next();
                    }),
                    next => wait(inflightFlushFrequencyMS * 2, next),
                    next => {
                        assert.strictEqual(scuba.getInflightsForBucket(bucket), size);
                        return next();
                    },
                    next => fakeMetadataArchive(bucket, key, vID, {
                        archiveInfo: {},
                        restoreRequestedAt: new Date(0).toISOString(),
                        restoreRequestedDays: 7,
                    }, next),
                    // Simulate the real restore
                    next => putObjectWithCustomHeader(bucket, key, size, vID, err => {
                        assert.ifError(err);
                        return next();
                    }),
                    next => {
                        assert.strictEqual(scuba.getInflightsForBucket(bucket), size);
                        return next();
                    },
                    next => deleteVersionID(bucket, key, vID, size, next),
                    next => deleteBucket(bucket, next),
                ], done);
            });

        it('should allow a restore if the quota is full but the objet fits with its reserved storage space',
            done => {
                const bucket = 'quota-test-bucket15';
                const key = 'quota-test-object';
                const size = 1000;
                let vID = null;
                return async.series([
                    next => createBucket(bucket, true, next),
                    next => sendRequest(putQuotaVerb, '127.0.0.1:8000', `/${bucket}/?quota=true`,
                        JSON.stringify(quota), config).then(() => next()).catch(err => next(err)),
                    next => putObject(bucket, key, size, (err, val) => {
                        assert.ifError(err);
                        vID = val.VersionId;
                        return next();
                    }),
                    next => wait(inflightFlushFrequencyMS * 2, next),
                    next => {
                        assert.strictEqual(scuba.getInflightsForBucket(bucket), size);
                        return next();
                    },
                    next => fakeMetadataArchive(bucket, key, vID, {
                        archiveInfo: {},
                        restoreRequestedAt: new Date(0).toISOString(),
                        restoreRequestedDays: 7,
                    }, next),
                    // Put an object, the quota should be exceeded
                    next => putObject(bucket, `${key}-2`, size, err => {
                        assert.strictEqual(err.code, 'QuotaExceeded');
                        return next();
                    }),
                    // Simulate the real restore
                    next => putObjectWithCustomHeader(bucket, key, size, vID, err => {
                        assert.ifError(err);
                        return next();
                    }),
                    next => {
                        assert.strictEqual(scuba.getInflightsForBucket(bucket), size);
                        return next();
                    },
                    next => deleteVersionID(bucket, key, vID, size, next),
                    next => deleteBucket(bucket, next),
                ], done);
            });
    });
