const async = require('async');
const assert = require('assert');
const { S3 } = require('aws-sdk');
const getConfig = require('../functional/aws-node-sdk/test/support/config');
const { Scuba: MockScuba, inflightFlushFrequencyMS } = require('../utilities/mock/Scuba');
const sendRequest = require('../functional/aws-node-sdk/test/quota/tooling').sendRequest;

let s3Client = null;
const quota = { quota: 1000 };

function wait(timeoutMs, cb) {
    setTimeout(cb, timeoutMs);
}

function createBucket(bucket, cb) {
    return s3Client.createBucket({
        Bucket: bucket,
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
    }, cb);
}

function copyObject(bucket, key, cb) {
    return s3Client.copyObject({
        Bucket: bucket,
        CopySource: `/${bucket}/${key}`,
        Key: `${key}-copy`,

    }, cb);
}

function deleteObject(bucket, key, cb) {
    return s3Client.deleteObject({
        Bucket: bucket,
        Key: key,
    }, err => {
        assert.ifError(err);
        return cb(err);
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
    ], err => callback(err, uploadId));
}

function abortMPU(bucket, key, uploadId, callback) {
    return s3Client.abortMultipartUpload({
        Bucket: bucket,
        Key: key,
        UploadId: uploadId,
    }, callback);
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
    ], err => callback(err, uploadId));
}

function restoreObject(bucket, key, callback) {
    return s3Client.restoreObject({
        Bucket: bucket,
        Key: key,
        RestoreRequest: {
            Days: 1,
        },
    }, callback);
}

describe('quota evaluation with scuba metrics', function t() {
    this.timeout(30000);
    const scuba = new MockScuba();
    const putQuotaVerb = 'PUT';

    before(done => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3Client = new S3(config);
        scuba.start();
        return wait(2000, done);
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
            next => createBucket(bucket, next),
            next => sendRequest(putQuotaVerb, '127.0.0.1:8000', `/${bucket}/?quota=true`, JSON.stringify(quota))
                .then(() => next()).catch(err => next(err)),
            next => putObject(bucket, key, size, err => {
                assert.strictEqual(err.code, 'QuotaExceeded');
                return next();
            }),
            next => deleteBucket(bucket, next),
        ], done);
    });

    it('should return QuotaExceeded when trying to CopyObject in a bucket with quota', done => {
        const bucket = 'quota-test-bucket2';
        const key = 'quota-test-object';
        const size = 900;
        return async.series([
            next => createBucket(bucket, next),
            next => sendRequest(putQuotaVerb, '127.0.0.1:8000', `/${bucket}/?quota=true`, JSON.stringify(quota))
                .then(() => next()).catch(err => next(err)),
            next => putObject(bucket, key, size, next),
            next => wait(inflightFlushFrequencyMS * 2, next),
            next => copyObject(bucket, key, err => {
                assert.strictEqual(err.code, 'QuotaExceeded');
                return next();
            }),
            next => deleteObject(bucket, key, next),
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
            next => createBucket(bucket, next),
            next => sendRequest(putQuotaVerb, '127.0.0.1:8000', `/${bucket}/?quota=true`, JSON.stringify(quota))
                .then(() => next()).catch(err => next(err)),
            next => objectMPU(bucket, key, parts, partSize, (err, _uploadId) => {
                uploadId = _uploadId;
                assert.strictEqual(err.code, 'QuotaExceeded');
                return next();
            }),
            next => abortMPU(bucket, key, uploadId, next),
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
            next => createBucket(bucket, next),
            next => sendRequest(putQuotaVerb, '127.0.0.1:8000', `/${bucket}/?quota=true`, JSON.stringify(quota))
                .then(() => next()).catch(err => next(err)),
            next => putObject(bucket, key, size, err => {
                assert.ifError(err);
                return next();
            }),
            next => deleteObject(bucket, key, next),
            next => deleteBucket(bucket, next),
        ], done);
    });

    it('should not evaluate quotas if the backend is not available', done => {
        scuba.stop();
        const bucket = 'quota-test-bucket5';
        const key = 'quota-test-object';
        const size = 1024;
        return async.series([
            next => createBucket(bucket, next),
            next => sendRequest(putQuotaVerb, '127.0.0.1:8000', `/${bucket}/?quota=true`, JSON.stringify(quota))
                .then(() => next()).catch(err => next(err)),
            next => putObject(bucket, key, size, err => {
                assert.ifError(err);
                return next();
            }),
            next => deleteObject(bucket, key, next),
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
            next => createBucket(bucket, next),
            next => sendRequest(putQuotaVerb, '127.0.0.1:8000', `/${bucket}/?quota=true`,
                JSON.stringify({ quota: Math.round(partSize * 2.5) }))
                    .then(() => next()).catch(err => next(err)),
            next => putObject(bucket, keyToCopy, partSize, next),
            next => uploadPartCopy(bucket, key, parts, partSize, inflightFlushFrequencyMS * 2, keyToCopy,
                (err, _uploadId) => {
                    uploadId = _uploadId;
                    assert.strictEqual(err.code, 'QuotaExceeded');
                    return next();
                }),
            next => abortMPU(bucket, key, uploadId, next),
            next => deleteObject(bucket, keyToCopy, next),
            next => deleteBucket(bucket, next),
        ], done);
    });

    it('should return QuotaExceeded when trying to restore an object in a bucket with quota', done => {
        const bucket = 'quota-test-bucket7';
        const key = 'quota-test-object';
        const size = 900;
        return async.series([
            next => createBucket(bucket, next),
            next => sendRequest(putQuotaVerb, '127.0.0.1:8000', `/${bucket}/?quota=true`, JSON.stringify(quota))
                .then(() => next()).catch(err => next(err)),
            next => putObject(bucket, key, size, err => {
                assert.ifError(err);
                return next();
            }),
            next => wait(inflightFlushFrequencyMS * 2, next),
            next => restoreObject(bucket, key, err => {
                assert.strictEqual(err.code, 'QuotaExceeded');
                return next();
            }),
            next => deleteObject(bucket, key, next),
            next => deleteBucket(bucket, next),
        ], done);
    });

    it('should allow writes after deleting data with quotas', done => {
        const bucket = 'quota-test-bucket8';
        const key = 'quota-test-object';
        const size = 400;
        return async.series([
            next => createBucket(bucket, next),
            next => sendRequest(putQuotaVerb, '127.0.0.1:8000', `/${bucket}/?quota=true`, JSON.stringify(quota))
                .then(() => next()).catch(err => next(err)),
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
            next => deleteObject(bucket, `${key}2`, next),
            next => wait(inflightFlushFrequencyMS * 2, next),
            next => putObject(bucket, `${key}4`, size, err => {
                assert.ifError(err);
                return next();
            }),
            next => deleteObject(bucket, `${key}1`, next),
            next => deleteObject(bucket, `${key}3`, next),
            next => deleteObject(bucket, `${key}4`, next),
            next => deleteBucket(bucket, next),
        ], done);
    });
    it('should not increase the inflights when the object is being rewritten with a smaller object', done => {
        const bucket = 'quota-test-bucket9';
        const key = 'quota-test-object';
        const size = 400;
        return async.series([
            next => createBucket(bucket, next),
            next => sendRequest(putQuotaVerb, '127.0.0.1:8000', `/${bucket}/?quota=true`, JSON.stringify(quota))
                .then(() => next()).catch(err => next(err)),
            next => putObject(bucket, key, size, err => {
                assert.ifError(err);
                return next();
            }),
            next => wait(inflightFlushFrequencyMS * 2, next),
            next => putObject(bucket, key, size - 100, err => {
                assert.ifError(err);
                return next();
            }),
            next => {
                assert.strictEqual(scuba.getInflightsForBucket(bucket), size);
                return next();
            },
            next => deleteObject(bucket, key, next),
            next => deleteBucket(bucket, next),
        ], done);
    });
});
