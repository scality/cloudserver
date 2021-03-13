const async = require('async');
const assert = require('assert');
const { S3 } = require('aws-sdk');

const MockUtapi = require('../utilities/mock/Utapi');
const getConfig = require('../functional/aws-node-sdk/test/support/config');
const WAIT_MS = 100;
let s3Client = null;

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
    }, (err, data) => {
        assert.ifError(err);
        return cb(err, data);
    });
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
function deleteObjects(bucket, keys, cb) {
    const objects = keys.map(key => {
        const keyObj = {
            Key: key,
        };
        return keyObj;
    });
    const params = {
        Bucket: bucket,
        Delete: {
            Objects: objects,
            Quiet: true,
        },
    };
    return s3Client.deleteObjects(params, err => {
        assert.ifError(err);
        return cb(err);
    });
}
function copyObject(bucket, key, cb) {
    return s3Client.copyObject({
        Bucket: bucket,
        CopySource: `/${bucket}/${key}`,
        Key: `${key}-copy`,
    }, err => {
        assert.ifError(err);
        return cb(err);
    });
}
function enableVersioning(bucket, enable, cb) {
    const versioningStatus = {
        Status: enable ? 'Enabled' : 'Disabled',
    };
    return s3Client.putBucketVersioning({
        Bucket: bucket,
        VersioningConfiguration: versioningStatus,
    }, err => {
        assert.ifError(err);
        return cb(err);
    });
}
function deleteVersionList(versionList, bucket, callback) {
    if (versionList === undefined || versionList.length === 0) {
        return callback();
    }
    const params = { Bucket: bucket, Delete: { Objects: [] } };
    versionList.forEach(version => {
        params.Delete.Objects.push({
            Key: version.Key, VersionId: version.VersionId,
        });
    });

    return s3Client.deleteObjects(params, callback);
}
function removeAllVersions(params, callback) {
    const bucket = params.Bucket;
    async.waterfall([
        cb => s3Client.listObjectVersions(params, cb),
        (data, cb) => deleteVersionList(data.DeleteMarkers, bucket,
            err => cb(err, data)),
        (data, cb) => deleteVersionList(data.Versions, bucket,
            err => cb(err, data)),
        (data, cb) => {
            if (data.IsTruncated) {
                const params = {
                    Bucket: bucket,
                    KeyMarker: data.NextKeyMarker,
                    VersionIdMarker: data.NextVersionIdMarker,
                };
                return removeAllVersions(params, cb);
            }
            return cb();
        },
    ], callback);
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
    ], callback);
}
function removeVersions(buckets, cb) {
    return async.each(buckets,
        (bucket, done) => removeAllVersions({ Bucket: bucket }, done), cb);
}
function getObject(bucket, key, cb) {
    return s3Client.getObject({
        Bucket: bucket,
        Key: key,
    }, (err, data) => {
        assert.ifError(err);
        return cb(err, data);
    });
}

describe('utapi v2 metrics incoming and outgoing bytes', function t() {
    this.timeout(30000);
    const utapi = new MockUtapi();

    function checkMetrics(inBytes, outBytes, objCount) {
        const accountMetrics = utapi.getAccountMetrics();
        assert(accountMetrics);
        assert.strictEqual(accountMetrics.incomingBytes, inBytes);
        assert.strictEqual(accountMetrics.outgoingBytes, outBytes);
        assert.strictEqual(accountMetrics.numberOfObjects, objCount);
    }

    before(() => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3Client = new S3(config);
        utapi.start();
    });
    afterEach(() => {
        utapi.reset();
    });
    after(() => {
        utapi.stop();
    });
    it('should set metrics for createBucket and deleteBucket', done => {
        const bucket = 'bucket1';
        async.series([
            next => createBucket(bucket, next),
            next => wait(WAIT_MS, () => {
                checkMetrics(0, 0, 0);
                next();
            }),
            next => deleteBucket(bucket, next),
            next => wait(WAIT_MS, () => {
                checkMetrics(0, 0, 0);
                next();
            }),
        ], done);
    });
    it('should set metrics for putObject and deleteObject', done => {
        const bucket = 'bucket2';
        const objectSize = 1024 * 1024;
        const obj1Size = objectSize * 1;
        const obj2Size = objectSize * 2;
        const key1 = '1.txt';
        const key2 = '2.txt';
        async.series([
            next => createBucket(bucket, next),
            next => putObject(bucket, key1, obj1Size, next),
            next => wait(WAIT_MS, () => {
                checkMetrics(obj1Size, 0, 1);
                next();
            }),
            next => putObject(bucket, key2, obj2Size, next),
            next => wait(WAIT_MS, () => {
                checkMetrics(obj1Size + obj2Size, 0, 2);
                next();
            }),
            next => deleteObject(bucket, key1, next),
            next => wait(WAIT_MS, () => {
                checkMetrics(obj2Size, 0, 1);
                next();
            }),
            next => deleteObject(bucket, key2, next),
            next => wait(WAIT_MS, () => {
                checkMetrics(0, 0, 0);
                next();
            }),
            next => deleteBucket(bucket, next),
        ], done);
    });
    it('should set metrics for copyObject', done => {
        const bucket = 'bucket3';
        const objectSize = 1024 * 1024 * 2;
        const key = '3.txt';
        async.series([
            next => createBucket(bucket, next),
            next => putObject(bucket, key, objectSize, next),
            next => copyObject(bucket, key, next),
            next => wait(WAIT_MS, () => {
                checkMetrics(objectSize * 2, 0, 2);
                next();
            }),
            next => deleteObject(bucket, key, next),
            next => wait(WAIT_MS, () => {
                checkMetrics(objectSize, 0, 1);
                next();
            }),
            next => deleteObject(bucket, `${key}-copy`, next),
            next => wait(WAIT_MS, () => {
                checkMetrics(0, 0, 0);
                next();
            }),
            next => deleteBucket(bucket, next),
        ], done);
    });
    it('should set metrics for getObject', done => {
        const bucket = 'bucket4';
        const objectSize = 1024 * 1024 * 2;
        const key = '4.txt';
        async.series([
            next => createBucket(bucket, next),
            next => putObject(bucket, key, objectSize, next),
            next => getObject(bucket, key, next),
            next => wait(WAIT_MS, () => {
                checkMetrics(objectSize, objectSize, 1);
                next();
            }),
            next => deleteObject(bucket, key, next),
            next => wait(WAIT_MS, () => {
                checkMetrics(0, objectSize, 0);
                next();
            }),
            next => deleteBucket(bucket, next),
        ], done);
    });
    it('should set metrics for multiObjectDelete', done => {
        const bucket = 'bucket5';
        const objectSize = 1024 * 1024;
        const obj1Size = objectSize * 2;
        const obj2Size = objectSize * 1;
        const key1 = '1.txt';
        const key2 = '2.txt';
        async.series([
            next => createBucket(bucket, next),
            next => putObject(bucket, key1, obj1Size, next),
            next => wait(WAIT_MS, next),
            next => putObject(bucket, key2, obj2Size, next),
            next => wait(WAIT_MS, () => {
                checkMetrics(obj1Size + obj2Size, 0, 2);
                next();
            }),
            next => deleteObjects(bucket, [key1, key2], next),
            next => wait(WAIT_MS, () => {
                checkMetrics(0, 0, 0);
                next();
            }),
            next => deleteBucket(bucket, next),
        ], done);
    });
    it('should set metrics for multiPartUpload', done => {
        const bucket = 'bucket6';
        const partSize = 1024 * 1024 * 6;
        const parts = 2;
        const key = '6.txt';
        async.series([
            next => createBucket(bucket, next),
            next => objectMPU(bucket, key, parts, partSize, next),
            next => wait(WAIT_MS, () => {
                checkMetrics(partSize * parts, 0, 1);
                next();
            }),
            next => deleteObject(bucket, key, next),
            next => wait(WAIT_MS, () => {
                checkMetrics(0, 0, 0);
                next();
            }),
            next => deleteBucket(bucket, next),
        ], done);
    });
    it('should set metrics in versioned bucket', done => {
        const bucket = 'bucket7';
        const objectSize = 1024 * 1024;
        const key = '7.txt';
        async.series([
            next => createBucket(bucket, next),
            next => enableVersioning(bucket, true, next),
            next => putObject(bucket, key, objectSize, next),
            next => wait(WAIT_MS, next),
            next => putObject(bucket, key, objectSize, next),
            next => wait(WAIT_MS, () => {
                checkMetrics(objectSize * 2, 0, 2);
                next();
            }),
            next => removeVersions([bucket], next),
            next => wait(WAIT_MS, () => {
                checkMetrics(0, 0, 0);
                next();
            }),
            next => deleteBucket(bucket, next),
        ], done);
    });
    it('should set metrics for multipartUpload in a versioned bucket', done => {
        const bucket = 'bucket8';
        const partSize = 1024 * 1024 * 6;
        const parts = 2;
        const key = '8.txt';
        async.series([
            next => createBucket(bucket, next),
            next => enableVersioning(bucket, true, next),
            next => objectMPU(bucket, key, parts, partSize, next),
            next => objectMPU(bucket, key, parts, partSize, next),
            next => wait(WAIT_MS, () => {
                checkMetrics(partSize * parts * 2, 0, 2);
                next();
            }),
            next => removeVersions([bucket], next),
            next => wait(WAIT_MS, () => {
                checkMetrics(0, 0, 0);
                next();
            }),
            next => deleteBucket(bucket, next),
        ], done);
    });
});
